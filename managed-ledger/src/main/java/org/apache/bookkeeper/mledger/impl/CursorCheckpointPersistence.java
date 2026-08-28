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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.AckState;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.AckStateRef;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.CursorCheckpoint;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongProperty;

@Slf4j
class CursorCheckpointPersistence {

    private final CursorCheckpointLog writer;
    private final ReadWriteLock lock;
    private final BookKeeper bookKeeper;
    private final BookKeeper.DigestType digestType;
    private final byte[] password;
    @Getter
    private final boolean perLedgerEntryPersistEnabled;

    // Position of each msg ledger's latest checkpoint in the cursor ledger; the AckStateRef
    // targets are derived from this index.
    private final Long2ObjectOpenHashMap<Position> lastCheckpointPos = new Long2ObjectOpenHashMap<>();
    private volatile CompletableFuture<CursorCheckpointLog.AppendResult> lastPersist =
            CompletableFuture.completedFuture(null);

    public CursorCheckpointPersistence(CursorCheckpointLog writer, ReadWriteLock lock,
                                         BookKeeper bookKeeper,
                                         BookKeeper.DigestType digestType, byte[] password,
                                         boolean perLedgerEntryPersistEnabled) {
        this.writer = writer;
        this.lock = lock;
        this.bookKeeper = bookKeeper;
        this.digestType = digestType;
        this.password = password;
        this.perLedgerEntryPersistEnabled = perLedgerEntryPersistEnabled;
    }

    public void setZkCheckpointHint(long cursorLedgerId, long entryId) {
        writer.setZkCheckpointHint(cursorLedgerId, entryId);
    }

    public synchronized CompletableFuture<CursorCheckpointLog.AppendResult> persist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            ManagedCursorImpl cursor) {
        lastPersist = lastPersist
                .exceptionally(e -> null)
                .thenCompose(ignored -> doPersist(lh, mdPos, properties, cursor));
        return lastPersist;
    }

    /**
     * Immutable snapshot of cursor state for one flush. Building reads only from this snapshot,
     * not live cursor state, so acks arriving during async appends don't wedge the flush.
     */
    private static final class PersistContext {
        final long mdLedgerId;
        // Ledgers whose checkpoint is written by this flush (dirty + self-healed positionless
        // active ledgers), excluding the mark-delete ledger.
        final Set<Long> flushedLedgers;
        // Ledgers that may be referenced by this flush's checkpoints.
        final Set<Long> activeLedgers;
        final Map<Long, byte[]> dirtyBitmaps;
        final Map<Long, List<BatchedEntryDeletionIndexInfo>> dirtyBatchAcks;
        final byte[] mdBitmap;
        final List<BatchedEntryDeletionIndexInfo> mdBatchAcks;

        PersistContext(long mdLedgerId, Set<Long> flushedLedgers, Set<Long> activeLedgers,
                       Map<Long, byte[]> dirtyBitmaps,
                       Map<Long, List<BatchedEntryDeletionIndexInfo>> dirtyBatchAcks,
                       byte[] mdBitmap, List<BatchedEntryDeletionIndexInfo> mdBatchAcks) {
            this.mdLedgerId = mdLedgerId;
            this.flushedLedgers = flushedLedgers;
            this.activeLedgers = activeLedgers;
            this.dirtyBitmaps = dirtyBitmaps;
            this.dirtyBatchAcks = dirtyBatchAcks;
            this.mdBitmap = mdBitmap;
            this.mdBatchAcks = mdBatchAcks;
        }
    }

    private PersistContext createPersistContext(ManagedCursorImpl cursor, Position mdPos) {
        lock.readLock().lock();
        try {
            long mdLedgerId = mdPos.getLedgerId();
            Set<Long> clearedDirtyLedgers = cursor.individualDeletedMessages.snapshotAndClearDirtyLedgers();
            Set<Long> flushedLedgers = new HashSet<>(clearedDirtyLedgers);
            flushedLedgers.remove(mdLedgerId);

            Set<Long> activeLedgers = new HashSet<>();
            cursor.individualDeletedMessages.forEachActiveLedger(activeLedgers::add);
            Map<Long, List<BatchedEntryDeletionIndexInfo>> batchAcksByLedger = groupBatchAcksByLedger(cursor);
            batchAcksByLedger.keySet().forEach(activeLedgers::add);

            // Self-heal: active ledgers without a persisted checkpoint are flushed this round,
            // otherwise they'd have no position to reference.
            for (long id : activeLedgers) {
                if (id != mdLedgerId && !lastCheckpointPos.containsKey(id)) {
                    flushedLedgers.add(id);
                }
            }

            Map<Long, byte[]> dirtyBitmaps = new HashMap<>();
            Map<Long, List<BatchedEntryDeletionIndexInfo>> dirtyBatchAcks = new HashMap<>();
            for (long id : flushedLedgers) {
                dirtyBitmaps.put(id, cursor.individualDeletedMessages.bitmapOf(id));
                dirtyBatchAcks.put(id, batchAcksByLedger.getOrDefault(id, Collections.emptyList()));
            }
            byte[] mdBitmap = cursor.individualDeletedMessages.bitmapOf(mdLedgerId);
            List<BatchedEntryDeletionIndexInfo> mdBatchAcks =
                    batchAcksByLedger.getOrDefault(mdLedgerId, Collections.emptyList());
            return new PersistContext(mdLedgerId, flushedLedgers, activeLedgers, dirtyBitmaps,
                    dirtyBatchAcks, mdBitmap, mdBatchAcks);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static Map<Long, List<BatchedEntryDeletionIndexInfo>> groupBatchAcksByLedger(
            ManagedCursorImpl cursor) {
        Map<Long, List<BatchedEntryDeletionIndexInfo>> result = new HashMap<>();
        if (cursor.batchDeletedIndexes == null) {
            return result;
        }
        cursor.batchDeletedIndexes.forEach((position, bitSet) -> {
            List<BatchedEntryDeletionIndexInfo> list =
                    result.computeIfAbsent(position.getLedgerId(), k -> new ArrayList<>());
            BatchedEntryDeletionIndexInfo.Builder infoBuilder = BatchedEntryDeletionIndexInfo.newBuilder();
            infoBuilder.getPositionBuilder()
                    .setLedgerId(position.getLedgerId())
                    .setEntryId(position.getEntryId());
            bitSet.stream().forEach(infoBuilder::addDeleteSet);
            list.add(infoBuilder.build());
        });
        return result;
    }

    private CompletableFuture<CursorCheckpointLog.AppendResult> doPersist(
            LedgerHandle lh, Position mdPos, Map<String, Long> properties,
            ManagedCursorImpl cursor) {
        PersistContext ctx = createPersistContext(cursor, mdPos);

        // Drop positions for ledgers below mark-delete that are no longer active. Active ledgers
        // are kept: batch-index entries may still be in memory before the align cleanup runs.
        lock.writeLock().lock();
        try {
            lastCheckpointPos.keySet().removeIf(id -> id < ctx.mdLedgerId && !ctx.activeLedgers.contains(id));
        } finally {
            lock.writeLock().unlock();
        }

        List<Long> dirtyOrder = new ArrayList<>(ctx.flushedLedgers);
        Collections.sort(dirtyOrder);
        // mdLedger is written first so other checkpoints in this flush can reference its position.
        List<Long> writeOrder = new ArrayList<>(dirtyOrder.size() + 1);
        writeOrder.add(ctx.mdLedgerId);
        writeOrder.addAll(dirtyOrder);

        final Set<Long> appendedLedgers = new HashSet<>();
        CompletableFuture<CursorCheckpointLog.AppendResult> chain =
                CompletableFuture.completedFuture(null);
        for (long ledgerId : writeOrder) {
            chain = chain.thenCompose(ignored -> {
                byte[] bitmap = ledgerId == ctx.mdLedgerId ? ctx.mdBitmap : ctx.dirtyBitmaps.get(ledgerId);
                List<BatchedEntryDeletionIndexInfo> batchAcks =
                        ledgerId == ctx.mdLedgerId ? ctx.mdBatchAcks : ctx.dirtyBatchAcks.get(ledgerId);
                CursorCheckpoint checkpoint = buildCheckpoint(ctx, ledgerId, bitmap, batchAcks,
                        mdPos, properties);
                return writer.appendCheckpoint(lh, checkpoint).thenApply(result -> {
                    lock.writeLock().lock();
                    try {
                        Position persistedPos = PositionFactory.create(lh.getId(), result.commitEntryId());
                        recordCheckpointPos(ledgerId, persistedPos);
                        appendedLedgers.add(ledgerId);
                    } finally {
                        lock.writeLock().unlock();
                    }
                    return result;
                });
            });
        }

        return chain.exceptionally(error -> {
            restoreDirtyForFailedLedgers(cursor, ctx.flushedLedgers, appendedLedgers);
            throw new CompletionException(error);
        });
    }

    /**
     * Re-marks dirty for ledgers whose checkpoint was not appended, so the next flush retries them.
     */
    /**
     * Records the position of a msg ledger's latest checkpoint. Persists are serialized
     * ({@link #persist} chains behind the previous one), so puts normally arrive in order;
     * the monotonic guard is defense-in-depth: a stale completion must never regress the
     * index, otherwise a later AckStateRef would point at an older (smaller) ack bitmap
     * and the acks recorded in the newer checkpoint would be lost on recovery.
     */
    @VisibleForTesting
    void recordCheckpointPos(long msgLedgerId, Position pos) {
        Position prev = lastCheckpointPos.get(msgLedgerId);
        if (prev == null || pos.compareTo(prev) > 0) {
            lastCheckpointPos.put(msgLedgerId, pos);
        }
    }

    @VisibleForTesting
    Position checkpointPosOf(long msgLedgerId) {
        return lastCheckpointPos.get(msgLedgerId);
    }

    private void restoreDirtyForFailedLedgers(ManagedCursorImpl cursor, Set<Long> flushedLedgers,
                                              Set<Long> appendedLedgers) {
        Set<Long> failedLedgers = new HashSet<>(flushedLedgers);
        failedLedgers.removeAll(appendedLedgers);
        if (failedLedgers.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            cursor.individualDeletedMessages.restoreDirtyLedgers(failedLedgers);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ============================ recover ============================

    public CompletableFuture<RecoveredCheckpoint> recover(LedgerHandle lh) {
        return writer.readLatest(lh).thenCompose(state -> {
            if (state.isLegacy()) {
                return CompletableFuture.completedFuture(RecoveredCheckpoint.legacy(state.legacyBytes));
            }
            CursorCheckpoint cp = state.checkpoint;
            return fetchAckStateRefs(cp, lh).thenApply(fetched -> {
                validateRecoveredAckData(cp, fetched);
                rebuildLastCheckpointPos(lh, state.commitEntryId, cp);
                return RecoveredCheckpoint.of(cp, fetched);
            });
        });
    }

    /** Validates no duplicate msgLedgerIds and that all refs resolved. Fails fast on corruption. */
    private static void validateRecoveredAckData(CursorCheckpoint cp, Map<Long, AckStateData> fetched) {
        Set<Long> expected = new HashSet<>(cp.getAckStatesCount() + cp.getAckStateRefsCount());
        for (AckState ackState : cp.getAckStatesList()) {
            if (!expected.add(ackState.getMsgLedgerId())) {
                throw new RuntimeException(new ManagedLedgerException(
                        "Recovery inconsistency: duplicate msgLedgerId " + ackState.getMsgLedgerId()
                                + " in inline ack states"));
            }
        }
        for (AckStateRef ref : cp.getAckStateRefsList()) {
            if (!expected.add(ref.getMsgLedgerId())) {
                throw new RuntimeException(new ManagedLedgerException(
                        "Recovery inconsistency: msgLedgerId " + ref.getMsgLedgerId()
                                + " appears both inline and in ack state refs"));
            }
        }
        if (!fetched.keySet().equals(expected)) {
            throw new RuntimeException(new ManagedLedgerException(
                    "Recovery inconsistency: expected ack states for " + expected
                            + ", got " + fetched.keySet()));
        }
    }

    /** Rebuilds lastCheckpointPos from the recovered checkpoint so the first persist can emit refs. */
    private void rebuildLastCheckpointPos(LedgerHandle lh, long commitEntryId, CursorCheckpoint cp) {
        lock.writeLock().lock();
        try {
            lastCheckpointPos.clear();
            Position inlinePos = PositionFactory.create(lh.getId(), commitEntryId);
            for (AckState ackState : cp.getAckStatesList()) {
                lastCheckpointPos.put(ackState.getMsgLedgerId(), inlinePos);
            }
            for (AckStateRef ref : cp.getAckStateRefsList()) {
                lastCheckpointPos.put(ref.getMsgLedgerId(),
                        PositionFactory.create(ref.getCursorLedgerId(), ref.getEntryId()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public CompletableFuture<RecoveredCheckpoint> recoverWithHint(
            LedgerHandle lh, long hintCursorLedgerId, long hintEntryId) {
        if (hintCursorLedgerId >= 0 && hintEntryId >= 0) {
            setZkCheckpointHint(hintCursorLedgerId, hintEntryId);
        }
        return recover(lh);
    }

    private CompletableFuture<Map<Long, AckStateData>> fetchAckStateRefs(CursorCheckpoint cp, LedgerHandle lh) {
        Map<Long, AckStateData> result = new HashMap<>();
        for (AckState ackState : cp.getAckStatesList()) {
            result.put(ackState.getMsgLedgerId(), toAckStateData(ackState));
        }
        List<AckStateRef> refs = cp.getAckStateRefsList();
        if (refs.isEmpty()) {
            return CompletableFuture.completedFuture(result);
        }
        List<CompletableFuture<AckStateData>> fetches = new ArrayList<>(refs.size());
        for (AckStateRef ref : refs) {
            fetches.add(fetchAckState(ref, lh));
        }
        return CompletableFuture.allOf(fetches.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    for (int i = 0; i < refs.size(); i++) {
                        result.put(refs.get(i).getMsgLedgerId(), fetches.get(i).join());
                    }
                    return result;
                });
    }

    private CompletableFuture<AckStateData> fetchAckState(AckStateRef ref, LedgerHandle lh) {
        return readCheckpointFromLedger(lh, ref.getCursorLedgerId(), ref.getEntryId())
                .thenApply(cp -> extractAckState(cp, ref));
    }

    private static AckStateData extractAckState(CursorCheckpoint cp, AckStateRef ref) {
        for (AckState ackState : cp.getAckStatesList()) {
            if (ackState.getMsgLedgerId() == ref.getMsgLedgerId()) {
                return toAckStateData(ackState);
            }
        }
        throw new RuntimeException(new ManagedLedgerException(
                "AckStateRef target " + ref + " does not contain msgLedgerId "
                        + ref.getMsgLedgerId()));
    }

    private static AckStateData toAckStateData(AckState ackState) {
        return new AckStateData(
                ackState.hasAckBitmap() ? ackState.getAckBitmap().toByteArray() : null,
                ackState.getBatchAcksList() != null
                        ? ackState.getBatchAcksList() : Collections.emptyList());
    }

    private CompletableFuture<CursorCheckpoint> readCheckpointFromLedger(
            LedgerHandle lh, long ledgerId, long entryId) {
        CompletableFuture<CursorCheckpoint> future = new CompletableFuture<>();
        if (ledgerId == lh.getId()) {
            // Refs into the recovered cursor ledger itself can reuse the already-open handle.
            writer.readAt(lh, entryId)
                    .whenComplete((state, error) -> completeCheckpointRead(future, ledgerId, entryId, state, error));
            return future;
        }
        bookKeeper.asyncOpenLedgerNoRecovery(ledgerId, digestType, password, (rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            writer.readAt(handle, entryId).whenComplete((state, error) -> {
                handle.asyncClose((closeRc, closeHandle, closeCtx) -> {}, null);
                completeCheckpointRead(future, ledgerId, entryId, state, error);
            });
        }, null);
        return future;
    }

    private static void completeCheckpointRead(CompletableFuture<CursorCheckpoint> future, long ledgerId,
                                               long entryId, CursorCheckpointLog.RecoveredState state,
                                               Throwable error) {
        if (error != null) {
            future.completeExceptionally(error);
        } else if (state.isLegacy()) {
            future.completeExceptionally(new ManagedLedgerException(
                    "AckStateRef target entry " + entryId + " in ledger " + ledgerId
                            + " is not a checkpoint"));
        } else {
            future.complete(state.checkpoint);
        }
    }

    // ============================ checkpoint builder ============================

    /**
     * Builds a checkpoint for one msg ledger: inline ack state plus refs to every other active
     * ledger's latest position. Refs stay valid because cursor ledgers are only GC'd once
     * mark-delete passes the last ledger holding acks.
     */
    private CursorCheckpoint buildCheckpoint(PersistContext ctx, long msgLedgerId, byte[] bitmap,
                                             List<BatchedEntryDeletionIndexInfo> batchAcks,
                                             Position mdPos, Map<String, Long> properties) {
        CursorCheckpoint.Builder cpBuilder = CursorCheckpoint.newBuilder()
                .setMarkDeleteLedgerId(mdPos.getLedgerId())
                .setMarkDeleteEntryId(mdPos.getEntryId());
        if (properties != null) {
            properties.forEach((name, value) -> {
                cpBuilder.addProperties(LongProperty.newBuilder().setName(name).setValue(value).build());
            });
        }
        addAckState(cpBuilder, msgLedgerId, bitmap, batchAcks);
        lock.readLock().lock();
        try {
            for (long id : ctx.activeLedgers) {
                if (id != msgLedgerId) {
                    Position pos = lastCheckpointPos.get(id);
                    if (pos == null) {
                        if (ctx.flushedLedgers.contains(id)) {
                            continue;
                        }
                        throw new IllegalStateException(
                                "Missing lastCheckpointPos for active msgLedgerId " + id);
                    }
                    cpBuilder.addAckStateRefs(AckStateRef.newBuilder()
                            .setMsgLedgerId(id)
                            .setCursorLedgerId(pos.getLedgerId())
                            .setEntryId(pos.getEntryId())
                            .build());
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return cpBuilder.build();
    }

    private static void addAckState(CursorCheckpoint.Builder cp, long msgLedgerId, byte[] bitmap,
                                    List<BatchedEntryDeletionIndexInfo> batchAcks) {
        AckState.Builder ackStateBuilder = AckState.newBuilder().setMsgLedgerId(msgLedgerId);
        if (bitmap != null && bitmap.length > 0) {
            ackStateBuilder.setAckBitmap(ByteString.copyFrom(bitmap));
        }
        if (batchAcks != null && !batchAcks.isEmpty()) {
            ackStateBuilder.addAllBatchAcks(batchAcks);
        }
        cp.addAckStates(ackStateBuilder.build());
    }

    // ============================ DTOs ============================

    record AckStateData(byte[] ackBitmap, List<BatchedEntryDeletionIndexInfo> batchAcks) {}

    record RecoveredCheckpoint(CursorCheckpoint checkpoint,
                                Map<Long, AckStateData> ackData,
                                byte[] legacyBytes) {
        static RecoveredCheckpoint of(CursorCheckpoint cp, Map<Long, AckStateData> ackData) {
            return new RecoveredCheckpoint(cp, ackData, null);
        }

        static RecoveredCheckpoint legacy(byte[] bytes) {
            return new RecoveredCheckpoint(null, null, bytes);
        }

        boolean isLegacy() {
            return legacyBytes != null;
        }
    }
}
