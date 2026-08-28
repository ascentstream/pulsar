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

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.CursorCheckpoint;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.CursorCheckpointChunk;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.CursorLogEntry;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
class CursorCheckpointLog {

    private static final long MAX_SCAN_BACK = 1000L;

    private final int maxEntrySize;
    private final int chunkEnvelopeOverhead;
    private volatile long zkCheckpointLedgerId = -1;
    private volatile long zkCheckpointEntryId = -1;

    CursorCheckpointLog(int maxEntrySize) {
        if (maxEntrySize < 1024) {
            throw new IllegalArgumentException("maxEntrySize must be at least 1024 bytes");
        }
        this.maxEntrySize = maxEntrySize;
        CursorLogEntry probe = CursorLogEntry.newBuilder()
                .setCheckpointChunk(CursorCheckpointChunk.newBuilder()
                        .setPartIndex(0).setPartCount(1)
                        .setCheckpointBytes(ByteString.copyFrom(new byte[0]))
                        .build())
                .build();
        this.chunkEnvelopeOverhead = probe.toByteArray().length + 8;
    }

    void setZkCheckpointHint(long cursorLedgerId, long entryId) {
        this.zkCheckpointLedgerId = cursorLedgerId;
        this.zkCheckpointEntryId = entryId;
    }

    CompletableFuture<AppendResult> appendCheckpoint(LedgerHandle lh, CursorCheckpoint checkpoint) {
        byte[] checkpointBytes = checkpoint.toByteArray();
        CursorLogEntry envelope;
        try {
            envelope = CursorLogEntry.newBuilder()
                    .setCheckpoint(CursorCheckpoint.parseFrom(checkpointBytes))
                    .build();
        } catch (Exception e) {
            return FutureUtil.failedFuture(new ManagedLedgerException("Failed to parse checkpoint", e));
        }
        byte[] data = envelope.toByteArray();
        if (data.length <= maxEntrySize) {
            return addEntry(lh, data).thenApply(entryId -> {
                log.debug("Appended checkpoint, ledgerId: {}, entryId: {}, size: {}",
                        lh.getId(), entryId, data.length);
                return new AppendResult(data.length, entryId);
            });
        }
        int maxPayloadSize = maxEntrySize - chunkEnvelopeOverhead;
        int partCount = (checkpointBytes.length + maxPayloadSize - 1) / maxPayloadSize;
        log.debug("Appending chunked checkpoint, ledgerId: {}, checkpointBytes: {}, partCount: {}",
                lh.getId(), checkpointBytes.length, partCount);
        List<CompletableFuture<Long>> futures = new ArrayList<>(partCount);
        int offset = 0;
        for (int i = 0; i < partCount; i++) {
            int length = Math.min(maxPayloadSize, checkpointBytes.length - offset);
            byte[] payload = new byte[length];
            System.arraycopy(checkpointBytes, offset, payload, 0, length);
            offset += length;
            CursorLogEntry part = CursorLogEntry.newBuilder()
                    .setCheckpointChunk(CursorCheckpointChunk.newBuilder()
                            .setPartIndex(i).setPartCount(partCount)
                            .setCheckpointBytes(ByteString.copyFrom(payload))
                            .build())
                    .build();
            futures.add(addEntry(lh, part.toByteArray()));
        }
        CompletableFuture<Long> lastPartFuture = futures.get(partCount - 1);
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenCompose(ignored -> lastPartFuture
                        .thenApply(lastEntryId -> {
                            log.debug("Appended chunked checkpoint, ledgerId: {}, lastEntryId: {}, "
                                            + "partCount: {}, totalBytes: {}",
                                    lh.getId(), lastEntryId, partCount, checkpointBytes.length);
                            return new AppendResult(checkpointBytes.length, lastEntryId);
                        }));
    }

    CompletableFuture<RecoveredState> readLatest(LedgerHandle lh) {
        long last = lh.getLastAddConfirmed();
        if (last < 0) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Cursor ledger " + lh.getId() + " has no entries"));
        }
        log.debug("Recovering checkpoint, ledgerId: {}, lastEntryId: {}", lh.getId(), last);
        return recoverEntryAt(lh, last, true).thenCompose(decision -> {
            if (decision.shouldScanBack) {
                log.info("Scanning back for last complete checkpoint, ledgerId: {}, entryId: {}",
                        lh.getId(), last);
                return scanBack(lh, last);
            }
            return CompletableFuture.completedFuture(decision.state);
        });
    }

    /**
     * Recovers the checkpoint stored at a specific entry, assembling chunked checkpoints when
     * needed. Unlike {@link #readLatest}, this never scans back: the target of an ack state ref
     * must be a complete checkpoint at exactly the given entry, so anything else fails fast.
     */
    CompletableFuture<RecoveredState> readAt(LedgerHandle lh, long entryId) {
        return recoverEntryAt(lh, entryId, false).thenCompose(decision -> {
            if (decision.shouldScanBack) {
                return FutureUtil.failedFuture(new ManagedLedgerException(
                        "No complete checkpoint at entry " + entryId + " in ledger " + lh.getId()));
            }
            return CompletableFuture.completedFuture(decision.state);
        });
    }

    /**
     * Reads and decodes the entry at {@code entryId}. When {@code fallbackToScanBack} is set,
     * decode failures yield a scan-back decision (used by the tail/scan-back paths); otherwise they
     * propagate to the caller.
     */
    private CompletableFuture<RecoveryDecision> recoverEntryAt(LedgerHandle lh, long entryId,
                                                               boolean fallbackToScanBack) {
        CompletableFuture<RecoveryDecision> recovered = readEntry(lh, entryId)
                .thenCompose(bytes -> recoverEntry(lh, entryId, bytes));
        return fallbackToScanBack
                ? recovered.exceptionally(error -> {
                    log.warn("Failed to recover entry, ledgerId: {}, entryId: {}", lh.getId(), entryId, error);
                    return RecoveryDecision.scanBack();
                })
                : recovered;
    }

    private CompletableFuture<RecoveryDecision> recoverEntry(LedgerHandle lh, long entryId, byte[] bytes) {
        if (bytes == null) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Entry " + entryId + " in cursor ledger " + lh.getId() + " is empty"));
        }

        CursorLogEntry envelope;
        try {
            envelope = CursorLogEntry.parseFrom(bytes);
        } catch (Exception e) {
            if (isLegacyPositionInfo(bytes)) {
                // Legacy PositionInfo bytes can fail CursorLogEntry parsing.
                log.debug("Recovered legacy PositionInfo after CursorLogEntry parse failure, "
                                + "ledgerId: {}, entryId: {}", lh.getId(), entryId);
                return CompletableFuture.completedFuture(RecoveryDecision.recovered(RecoveredState.legacy(bytes)));
            }
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Invalid cursor log entry at ledger " + lh.getId() + " entry " + entryId
                            + ": failed to parse CursorLogEntry", e));
        }
        if (envelope.hasCheckpoint()) {
            CursorCheckpoint cp = envelope.getCheckpoint();
            log.debug("Recovered checkpoint, ledgerId: {}, entryId: {}, mdLedgerId: {}, mdEntryId: {}, "
                            + "ackStates: {}, ackStateRefs: {}",
                    lh.getId(), entryId, cp.getMarkDeleteLedgerId(), cp.getMarkDeleteEntryId(),
                    cp.getAckStatesCount(), cp.getAckStateRefsCount());
            return CompletableFuture.completedFuture(
                    RecoveryDecision.recovered(new RecoveredState(cp, entryId)));
        }
        if (envelope.hasCheckpointChunk()) {
            CursorCheckpointChunk chunk = envelope.getCheckpointChunk();
            int partCount = chunk.getPartCount();
            int partIndex = chunk.getPartIndex();
            if (partIndex >= 0 && partIndex < partCount) {
                return recoverFromChunk(lh, entryId, chunk);
            }
        }
        if (isLegacyPositionInfo(bytes)) {
            // Backward compatibility: legacy PositionInfo bytes can parse as an empty CursorLogEntry.
            log.debug("Recovered legacy PositionInfo from unknown CursorLogEntry payload, "
                            + "ledgerId: {}, entryId: {}", lh.getId(), entryId);
            return CompletableFuture.completedFuture(RecoveryDecision.recovered(RecoveredState.legacy(bytes)));
        }
        return FutureUtil.failedFuture(new ManagedLedgerException(
                "Invalid cursor log entry at ledger " + lh.getId() + " entry " + entryId
                        + ": neither checkpoint nor checkpointChunk"));
    }

    private CompletableFuture<RecoveryDecision> recoverFromChunk(
            LedgerHandle lh, long entryId, CursorCheckpointChunk chunk) {
        int partCount = chunk.getPartCount();
        int partIndex = chunk.getPartIndex();
        if (partIndex < 0 || partIndex >= partCount) {
            log.warn("Invalid chunk metadata, ledgerId: {}, entryId: {}, partIndex: {}, partCount: {}",
                    lh.getId(), entryId, partIndex, partCount);
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Invalid chunk metadata at ledger " + lh.getId() + " entry " + entryId
                            + ": partIndex=" + partIndex + ", partCount=" + partCount));
        }
        if (partIndex != partCount - 1) {
            log.debug("Chunk is not last part, scanning back, ledgerId: {}, entryId: {}, partIndex: {}, partCount: {}",
                    lh.getId(), entryId, partIndex, partCount);
            return CompletableFuture.completedFuture(RecoveryDecision.scanBack());
        }
        return assemble(lh, entryId, partCount)
                .handle((cp, error) -> {
                    if (error != null) {
                        log.warn("Chunk assembly failed, scanning back, ledgerId: {}, entryId: {}, partCount: {}",
                                lh.getId(), entryId, partCount, error);
                        return RecoveryDecision.scanBack();
                    }
                    return RecoveryDecision.recovered(new RecoveredState(cp, entryId));
                });
    }

    private CompletableFuture<CursorCheckpoint> assemble(LedgerHandle lh, long lastPartEntryId, int partCount) {
        long firstId = lastPartEntryId - partCount + 1;
        if (firstId < 0) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Chunk assembly underflow: first part entry id " + firstId
                            + " (lastPartEntryId=" + lastPartEntryId + ", partCount=" + partCount + ")"));
        }
        return readEntryRange(lh, firstId, lastPartEntryId).thenApply(partsBytes -> {
            if (partsBytes.size() != partCount) {
                throw new RuntimeException(new ManagedLedgerException(
                        "Chunk assembly mismatch: expected " + partCount + ", got " + partsBytes.size()));
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < partCount; i++) {
                CursorLogEntry part;
                try {
                    part = CursorLogEntry.parseFrom(partsBytes.get(i));
                } catch (Exception e) {
                    throw new RuntimeException(new ManagedLedgerException("Failed to parse chunk part " + i, e));
                }
                if (!part.hasCheckpointChunk()) {
                    throw new RuntimeException(new ManagedLedgerException("Part " + i + " is not a chunk"));
                }
                CursorCheckpointChunk chunk = part.getCheckpointChunk();
                if (chunk.getPartIndex() != i || chunk.getPartCount() != partCount) {
                    throw new RuntimeException(new ManagedLedgerException(
                            "Part " + i + ": partIndex=" + chunk.getPartIndex()
                                    + " partCount=" + chunk.getPartCount() + ", expected " + i + "/" + partCount));
                }
                byte[] checkpointBytes = chunk.getCheckpointBytes().toByteArray();
                bos.write(checkpointBytes, 0, checkpointBytes.length);
            }
            try {
                CursorCheckpoint cp = CursorCheckpoint.parseFrom(bos.toByteArray());
                log.debug("Assembled chunked checkpoint, ledgerId: {}, partCount: {}, assembledBytes: {}",
                        lh.getId(), partCount, bos.size());
                return cp;
            } catch (Exception e) {
                throw new RuntimeException(new ManagedLedgerException("Failed to parse assembled checkpoint", e));
            }
        });
    }

    private CompletableFuture<RecoveredState> scanBack(LedgerHandle lh, long fromEntryId) {
        CompletableFuture<RecoveredState> hintFuture = null;
        if (zkCheckpointLedgerId >= 0 && zkCheckpointEntryId >= 0
                && zkCheckpointLedgerId == lh.getId() && zkCheckpointEntryId < fromEntryId) {
            log.debug("Trying ZK checkpoint hint before scan-back, ledgerId: {}, hintEntryId: {}, fromEntryId: {}",
                    lh.getId(), zkCheckpointEntryId, fromEntryId);
            hintFuture = recoverEntryAt(lh, zkCheckpointEntryId, false)
                    .thenCompose(decision -> decision.shouldScanBack
                            ? scanBackStep(lh, fromEntryId - 1)
                            : CompletableFuture.completedFuture(decision.state));
        }
        if (hintFuture != null) {
            return hintFuture.exceptionally(t -> {
                log.debug("ZK checkpoint hint failed, falling back to sequential scan-back, "
                                + "ledgerId: {}, hintEntryId: {}",
                        lh.getId(), zkCheckpointEntryId);
                return null;
            }).thenCompose(state ->
                    state != null ? CompletableFuture.completedFuture(state)
                            : scanBackStep(lh, fromEntryId - 1));
        }
        return scanBackStep(lh, fromEntryId - 1);
    }

    private CompletableFuture<RecoveredState> scanBackStep(LedgerHandle lh, long entryId) {
        long floor = lh.getLastAddConfirmed() - MAX_SCAN_BACK;
        if (entryId < 0 || entryId < floor) {
            log.warn("Scan-back exhausted without recoverable checkpoint, ledgerId: {}, entryId: {}, floor: {}",
                    lh.getId(), entryId, floor);
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "scanBack exhausted without finding a complete checkpoint"));
        }
        return recoverEntryAt(lh, entryId, true)
                .thenCompose(decision -> decision.shouldScanBack
                        ? scanBackStep(lh, entryId - 1)
                        : CompletableFuture.completedFuture(decision.state));
    }

    private static CompletableFuture<Long> addEntry(LedgerHandle lh, byte[] data) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        lh.asyncAddEntry(data, (rc, handle, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                future.complete(entryId);
            } else {
                future.completeExceptionally(BKException.create(rc));
            }
        }, null);
        return future;
    }

    private static CompletableFuture<byte[]> readEntry(LedgerHandle lh, long entryId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        lh.asyncReadEntries(entryId, entryId, (rc, lh1, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            if (!entries.hasMoreElements()) {
                future.complete(null);
                return;
            }
            future.complete(entries.nextElement().getEntry());
        }, null);
        return future;
    }

    private static CompletableFuture<List<byte[]>> readEntryRange(LedgerHandle lh, long firstId, long lastId) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        lh.asyncReadEntries(firstId, lastId, (rc, lh1, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            List<byte[]> result = new ArrayList<>();
            while (entries.hasMoreElements()) {
                result.add(entries.nextElement().getEntry());
            }
            future.complete(result);
        }, null);
        return future;
    }

    private static boolean isLegacyPositionInfo(byte[] bytes) {
        try {
            PositionInfo.parseFrom(bytes);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    record AppendResult(int totalBytes, long commitEntryId) {}

    private static final class RecoveryDecision {
        private final RecoveredState state;
        private final boolean shouldScanBack;

        private RecoveryDecision(RecoveredState state, boolean shouldScanBack) {
            this.state = state;
            this.shouldScanBack = shouldScanBack;
        }

        static RecoveryDecision recovered(RecoveredState state) {
            return new RecoveryDecision(state, false);
        }

        static RecoveryDecision scanBack() {
            return new RecoveryDecision(null, true);
        }
    }

    static final class RecoveredState {
        final CursorCheckpoint checkpoint;
        final long commitEntryId;
        final byte[] legacyBytes;

        private RecoveredState(CursorCheckpoint checkpoint, long commitEntryId) {
            this.checkpoint = checkpoint;
            this.commitEntryId = commitEntryId;
            this.legacyBytes = null;
        }

        private RecoveredState(byte[] legacyBytes) {
            this.checkpoint = null;
            this.commitEntryId = -1;
            this.legacyBytes = legacyBytes;
        }

        static RecoveredState legacy(byte[] bytes) {
            return new RecoveredState(bytes);
        }

        boolean isLegacy() {
            return legacyBytes != null;
        }
    }
}
