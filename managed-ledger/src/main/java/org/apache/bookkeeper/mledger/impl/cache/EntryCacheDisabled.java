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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;

/**
 * Implementation of cache that always read from BookKeeper.
 */
@Slf4j
public class EntryCacheDisabled implements EntryCache {
    private final ManagedLedgerImpl ml;
    private final ManagedLedgerInterceptor interceptor;

    public EntryCacheDisabled(ManagedLedgerImpl ml) {
        this.ml = ml;
        this.interceptor = ml.getManagedLedgerInterceptor();
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    @Override
    public boolean insert(Entry entry) {
        return false;
    }

    @Override
    public void invalidateEntries(Position lastPosition) {
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
    }

    @Override
    public void clear() {
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, long maxSizeBytes,
                               IntSupplier expectedReadCount, final AsyncCallbacks.ReadEntriesCallback callback,
                               Object ctx) {
        readEntries(lh, firstEntry, lastEntry, maxSizeBytes, callback, ctx);
    }

    private void readEntries(ReadHandle lh, long firstEntry, long lastEntry, long maxSizeBytes,
                             AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        ReadEntryUtils.readAsync(ml, lh, firstEntry, lastEntry, ml.isBatchReadEnabled(), maxSizeBytes)
                .thenApplyAsync(ledgerEntries -> {
                    List<Entry> entries = new ArrayList<>();
                    long totalSize = 0;
                    try {
                        for (LedgerEntry e : ledgerEntries) {
                            // Insert the entries at the end of the list (they will be unsorted for now)
                            EntryImpl entry = EntryImpl.create(e, interceptor, 0);
                            if (ml.getConfig().isPulsarMessageEntries()) {
                                entry.initializeMessageMetadataIfNeeded(ml.getName());
                            }
                            entries.add(entry);
                            totalSize += entry.getLength();
                        }
                    } finally {
                        ledgerEntries.close();
                    }
                    ml.getMbean().recordReadEntriesOpsCacheMisses(entries.size(), totalSize);
                    ml.getFactory().getMbean().recordCacheMiss(entries.size(), totalSize);
                    ml.getMbean().addReadEntriesSample(entries.size(), totalSize);

                    return entries;
                }, ml.getExecutor()).whenCompleteAsync((entries, exception) -> {
                    if (exception == null) {
                        try {
                            callback.readEntriesComplete(entries, ctx);
                        } catch (Throwable t) {
                            log.warn("[{}] Read callback failed for ledger {} entries {}-{}; the callback remains "
                                    + "responsible for releasing entries", getName(), lh.getId(), firstEntry,
                                    lastEntry, t);
                        }
                    } else {
                        try {
                            callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
                        } catch (Throwable t) {
                            log.warn("[{}] Read callback failed for ledger {} entries {}-{}", getName(), lh.getId(),
                                    firstEntry, lastEntry, t);
                        }
                    }
                }, ml.getExecutor());
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, Position position, AsyncCallbacks.ReadEntryCallback callback,
                               Object ctx) {
        asyncReadEntry(lh, position.getEntryId(), position.getEntryId(), NO_MAX_SIZE_LIMIT, () -> 0,
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object callbackCtx) {
                        Iterator<Entry> iterator = entries.iterator();
                        if (iterator.hasNext()) {
                            callback.readEntryComplete(iterator.next(), callbackCtx);
                        } else {
                            callback.readEntryFailed(new ManagedLedgerException("Could not read given position"),
                                    callbackCtx);
                        }
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object callbackCtx) {
                        if (!(exception instanceof ManagedLedgerException.TooManyRequestsException)) {
                            ml.invalidateLedgerHandle(lh);
                        }
                        callback.readEntryFailed(exception, callbackCtx);
                    }
                }, ctx);
    }

    @Override
    public long getSize() {
        return 0;
    }
}
