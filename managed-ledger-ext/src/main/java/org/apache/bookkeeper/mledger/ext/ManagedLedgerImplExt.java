/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.ext;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extended implementation of ManagedLedger with additional trimming capabilities.
 * This class extends ManagedLedgerImpl to add the asyncTrimConsumedLedgersBefore method.
 *
 * Note: This implementation uses reflection to access package-private fields and methods
 * from ManagedLedgerImpl. This is necessary because the trim logic requires access to
 * internal state that is not exposed through public APIs.
 */
public class ManagedLedgerImplExt extends ManagedLedgerImpl {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImplExt.class);
    private static final long TRIM_RETRY_DELAY_MS = 100;

    private final java.util.concurrent.ScheduledExecutorService scheduledExecutorForRetry;
    private final ManagedLedgerFactoryImpl factory;

    public ManagedLedgerImplExt(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
            final String name, final Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        this.factory = factory;
        this.scheduledExecutorForRetry = scheduledExecutor;
    }

    @Override
    public CompletableFuture<Void> asyncTrimConsumedLedgersBefore(long ledgerId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> internalTrimConsumedLedgersBefore(ledgerId, future));
        return future;
    }

    /**
     * Internal method to trim consumed ledgers before the specified ledgerId.
     * This method follows the same structure as internalTrimLedgers but uses
     * the specified ledgerId as the trim boundary instead of retention policies.
     *
     * Semantics:
     * - trimConsumedLedgersBefore(L4) where L4 is current ledger: delete L1, L2, L3, keep L4 → 1 left
     * - trimConsumedLedgersBefore(L3) where L3 is middle ledger: delete L1, L2, L3, keep L4 → 1 left
     * - trimConsumedLedgersBefore(L2) where L2 is middle ledger: delete L1, L2, keep L3, L4 → 2 left
     * - trimConsumedLedgersBefore(L3) where L1, L2, L4 exist (L3 gap): delete L1, L2, keep L4 → 1 left
     *
     * Key rule: For current ledger, keep it. For middle ledger, delete it too.
     * If ledgerId doesn't exist, use the next lower existing ledger as boundary.
     */
    @SuppressWarnings("unchecked")
    private void internalTrimConsumedLedgersBefore(long ledgerId, CompletableFuture<Void> future) {
        // Evict inactive offloaded ledgers (same as internalTrimLedgers)
        internalEvictOffloadedLedgers();

        // Check metadata service availability
        if (!factory.isMetadataServiceAvailable()) {
            future.completeExceptionally(new ManagedLedgerException.MetaStoreException(
                    "Metadata service is not available"));
            return;
        }

        // Ensure only one trimming operation is active
        CallbackMutex trimmerMutex = getTrimmerMutex();
        if (!trimmerMutex.tryLock()) {
            scheduleDeferredTrimmingBefore(ledgerId, future);
            return;
        }

        List<LedgerInfo> ledgersToDelete = new ArrayList<>();
        List<LedgerInfo> offloadedLedgersToDelete = new ArrayList<>();
        Optional<OffloadPolicies> optionalOffloadPolicies = getOffloadPoliciesIfAppendable();

        synchronized (this) {
            // Get ledgers map via reflection
            NavigableMap<Long, LedgerInfo> ledgersMap = ledgers;

            // Determine the actual ledger ID to use (adjust if original doesn't exist)
            long effectiveLedgerId;
            if (ledgersMap.containsKey(ledgerId)) {
                // Ledger exists, use it directly
                effectiveLedgerId = ledgerId;
            } else {
                // Ledger doesn't exist, find the appropriate boundary
                long lastLedgerId = ledgersMap.lastKey();
                if (ledgerId > lastLedgerId) {
                    // ledgerId is beyond all ledgers, use current ledger as boundary
                    effectiveLedgerId = currentLedger.getId();
                    log.info("[{}] Ledger {} does not exist (last ledger is {}), using current ledger {} as boundary",
                            name, ledgerId, lastLedgerId, effectiveLedgerId);
                } else {
                    // ledgerId is within the range but doesn't exist (e.g., gap)
                    // Use the greatest existing ledger that is less than ledgerId
                    Long lowerLedger = ledgersMap.lowerKey(ledgerId);
                    if (lowerLedger != null) {
                        effectiveLedgerId = lowerLedger;
                        log.info("[{}] Ledger {} does not exist, using next lower ledger {} as boundary",
                                name, ledgerId, effectiveLedgerId);
                    } else {
                        // No ledger is less than ledgerId (e.g., ledgerId < first ledger)
                        // Nothing to trim, return successfully
                        log.info("[{}] Ledger {} is less than first ledger, nothing to trim", name, ledgerId);
                        trimmerMutex.unlock();
                        future.complete(null);
                        return;
                    }
                }
            }
            final long actualLedgerId = effectiveLedgerId;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Start TrimConsumedLedgersBefore {}. ledgers={} totalSize={}",
                        name, ledgerId, ledgersMap.keySet(), getTotalSize());
            }

            // Check state
            State currentState = getState();
            if (currentState == State.Closed) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already closed", name);
                trimmerMutex.unlock();
                future.completeExceptionally(new ManagedLedgerException("Can't trim closed ledger"));
                return;
            }
            if (currentState == State.Fenced) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already fenced", name);
                trimmerMutex.unlock();
                future.completeExceptionally(new ManagedLedgerException("Can't trim fenced ledger"));
                return;
            }

            // The trim boundary ledger ID:
            // Semantics:
            // - If targeting current ledger: delete all ledgers BEFORE it (keep current ledger)
            // - If targeting middle ledger: delete the ledger AND all before it (do NOT keep boundary)
            final long trimBoundaryLedgerId = actualLedgerId;
            final boolean isTargetingCurrentLedger = (actualLedgerId == currentLedger.getId());

            log.info("[{}] Trim boundary: {}, current ledger: {}, isTargetingCurrent: {}",
                    name, trimBoundaryLedgerId, currentLedger.getId(), isTargetingCurrentLedger);

            // Calculate slowest reader position (same as internalTrimLedgers)
            long slowestReaderLedgerId = calculateSlowestReaderLedgerId();

            if (slowestReaderLedgerId < 0) {
                // Error in calculating slowest reader position
                trimmerMutex.unlock();
                future.completeExceptionally(new ManagedLedgerException("Couldn't find reader position"));
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Slowest consumer ledger id: {}, trimming before: {}",
                        name, slowestReaderLedgerId, trimBoundaryLedgerId);
            }

            if (slowestReaderLedgerId < trimBoundaryLedgerId) {
                log.debug("[{}] Cannot trim before {}: slowest reader is at {}",
                        name, trimBoundaryLedgerId, slowestReaderLedgerId);
                trimmerMutex.unlock();
                future.completeExceptionally(new ManagedLedgerException(
                        "Cannot trim: ledgers before " + trimBoundaryLedgerId + " are not fully consumed. "
                        + "Slowest reader is at ledger " + slowestReaderLedgerId));
                return;
            }

            // Collect ledgers to delete
            // - If targeting current ledger: delete all BEFORE boundary (keep boundary)
            // - If targeting middle ledger: delete boundary AND all BEFORE it (do not keep boundary)
            // headMap(toKey, inclusive): true=include toKey, false=exclude toKey
            Iterator<LedgerInfo> ledgerInfoIterator = ledgersMap.headMap(
                    trimBoundaryLedgerId, !isTargetingCurrentLedger).values().iterator();
            while (ledgerInfoIterator.hasNext()) {
                LedgerInfo ls = ledgerInfoIterator.next();
                if (ls.getLedgerId() == currentLedger.getId()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ledger {} skipped for deletion as it is currently being written to",
                                name, ls.getLedgerId());
                    }
                    break;
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Ledger {} will be deleted (before {})",
                            name, ls.getLedgerId(), trimBoundaryLedgerId);
                }
                ledgersToDelete.add(ls);
            }

            // Collect offloaded ledgers to delete
            for (LedgerInfo ls : ledgersMap.values()) {
                if (isOffloadedNeedsDelete(ls.getOffloadContext(), optionalOffloadPolicies)
                        && !ledgersToDelete.contains(ls)) {
                    log.debug("[{}] Ledger {} has been offloaded, bookkeeper ledger needs to be deleted",
                            name, ls.getLedgerId());
                    offloadedLedgersToDelete.add(ls);
                }
            }

            if (ledgersToDelete.isEmpty() && offloadedLedgersToDelete.isEmpty()) {
                trimmerMutex.unlock();
                future.complete(null);
                return;
            }

            CallbackMutex metadataMutex = getMetadataMutex();
            if (currentState == State.CreatingLedger || !metadataMutex.tryLock()) {
                scheduleDeferredTrimmingBefore(ledgerId, future);
                trimmerMutex.unlock();
                return;
            }

            try {
                advanceCursorsIfNecessary(ledgersToDelete);
            } catch (Exception e) {
                log.info("[{}] Error while advancing cursors during trim before {}",
                        name, trimBoundaryLedgerId, e.getMessage());
                metadataMutex.unlock();
                trimmerMutex.unlock();
                future.completeExceptionally(e);
                return;
            }

            doDeleteLedgers(ledgersToDelete);

            // Update offloaded ledgers metadata
            for (LedgerInfo ls : offloadedLedgersToDelete) {
                LedgerInfo.Builder newInfoBuilder = ls.toBuilder();
                newInfoBuilder.getOffloadContextBuilder().setBookkeeperDeleted(true);
                String driverName = OffloadUtils.getOffloadDriverName(ls,
                        config.getLedgerOffloader().getOffloadDriverName());
                Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(ls,
                        config.getLedgerOffloader().getOffloadDriverMetadata());
                OffloadUtils.setOffloadDriverMetadata(newInfoBuilder, driverName, driverMetadata);
                ledgersMap.put(ls.getLedgerId(), newInfoBuilder.build());
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating of ledgers list after trimming before {}", name, trimBoundaryLedgerId);
            }

            Stat currentLedgersStat = ledgersStat;

            store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), currentLedgersStat,
                    new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info("[{}] End TrimConsumedLedgersBefore {}. ledgers={} totalSize={}",
                            name, trimBoundaryLedgerId, ledgersMap.size(), getTotalSize());
                    ledgersStat = stat;
                    metadataMutex.unlock();
                    trimmerMutex.unlock();

                    notifyDeleteLedgerEvent(ledgersToDelete.toArray(new LedgerInfo[0]));
                    for (LedgerInfo ls : ledgersToDelete) {
                        log.info("[{}] Removing ledger {} - size: {}", name, ls.getLedgerId(), ls.getSize());
                        asyncDeleteLedger(ls.getLedgerId(), ls);
                    }

                    notifyDeleteLedgerEvent(offloadedLedgersToDelete.toArray(new LedgerInfo[0]));
                    for (LedgerInfo ls : offloadedLedgersToDelete) {
                        log.info("[{}] Deleting offloaded ledger {} from bookkeeper - size: {}", name, ls.getLedgerId(),
                                ls.getSize());
                        invalidateReadHandle(ls.getLedgerId());
                        asyncDeleteLedger(ls.getLedgerId(), DEFAULT_LEDGER_DELETE_RETRIES).thenAccept(__ -> {
                            log.info("[{}] Deleted and invalidated offloaded ledger {} from bookkeeper - size: {}",
                                    name, ls.getLedgerId(), ls.getSize());
                        }).exceptionally(ex -> {
                            log.error("[{}] Failed to delete offloaded ledger {} from bookkeeper - size: {}",
                                    name, ls.getLedgerId(), ls.getSize(), ex);
                            return null;
                        });
                    }

                    future.complete(null);
                }

                @Override
                public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                    log.warn("[{}] Failed to update the list of ledgers after trimming before {}",
                            name, trimBoundaryLedgerId, e);
                    metadataMutex.unlock();
                    trimmerMutex.unlock();
                    handleBadVersion(e);
                    future.completeExceptionally(e);
                }
            });
        }
    }

    private void scheduleDeferredTrimmingBefore(long ledgerId, CompletableFuture<Void> future) {
        scheduledExecutorForRetry.schedule(
                () -> executor.execute(() -> internalTrimConsumedLedgersBefore(ledgerId, future)),
                TRIM_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    private long calculateSlowestReaderLedgerId() {
        try {
            boolean hasDurableCursors = getCursors().hasDurableCursors();
            long currentLedgerId = currentLedger.getId();

            if (!hasDurableCursors) {
                return currentLedgerId + 1;
            }

            PositionImpl slowestReaderPosition = getCursors().getSlowestReaderPosition();
            if (slowestReaderPosition == null) {
                return -1;
            }

            long positionLedgerId = slowestReaderPosition.getLedgerId();
            long positionEntryId = slowestReaderPosition.getEntryId();

            NavigableMap<Long, LedgerInfo> ledgersMap = ledgers;
            LedgerInfo ledgerInfo = ledgersMap.get(positionLedgerId);

            if (ledgerInfo != null && ledgerInfo.getLedgerId() != currentLedgerId
                    && ledgerInfo.getEntries() == positionEntryId + 1) {
                return positionLedgerId + 1;
            }
            return positionLedgerId;
        } catch (Exception e) {
            log.error("[{}] Error calculating slowest reader position", name, e);
            return -1;
        }
    }

}
