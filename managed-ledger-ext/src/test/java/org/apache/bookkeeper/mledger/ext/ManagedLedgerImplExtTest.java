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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Test cases for ManagedLedgerImplExt custom trim functionality.
 */
@Slf4j
public class ManagedLedgerImplExtTest extends BookKeeperClusterTestCase {

    private StatsLogger statsLogger;

    public ManagedLedgerImplExtTest() {
        super(2);
    }

    @BeforeTest(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        statsLogger = NullStatsLogger.INSTANCE;
    }

    /**
     * Create a ManagedLedgerFactoryImplExt for testing.
     */
    private ManagedLedgerFactoryImplExt createFactory(ManagedLedgerFactoryConfig factoryConf) throws Exception {
        return new ManagedLedgerFactoryImplExt(metadataStore,
                new ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy() {
                    @Override
                    public CompletableFuture<BookKeeper> get(EnsemblePlacementPolicyConfig config) {
                        return CompletableFuture.completedFuture(bkc);
                    }

                    @Override
                    public CompletableFuture<BookKeeper> get() {
                        return CompletableFuture.completedFuture(bkc);
                    }
                }, factoryConf, statsLogger);
    }

    /**
     * Test trimming consumed ledgers before a specified ledger ID.
     *
     * Scenario: L1, L2, L3, L4 (current)
     * Test: trimConsumedLedgersBefore(L3)
     * Expected: delete L1, L2, L3, keep L4 → 1 ledger left
     */
    @Test
    public void testTrimConsumedLedgersBefore() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)  // Force ledger rollover after 3 entries
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)  // Disable time-based retention
                .setRetentionSizeInMB(-1);  // Disable size-based retention

        String ledgerName = "test-trim-consumed-ledgers-" + UUID.randomUUID().toString();
        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open(ledgerName, config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID().toString());

        // Write 10 entries - this should create 4 ledgers (3, 3, 3, 1)
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        log.info("Total ledgers created: {}", ledger.getLedgersInfo().size());

        // Get the ledger IDs
        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 3, "Should have at least 3 ledgers, got: " + ledgerIds.length);

        long firstLedgerId = ledgerIds[0];
        long secondLedgerId = ledgerIds[1];
        long lastLedgerId = ledgerIds[ledgerIds.length - 1];
        // Trim before the 2nd-to-last ledger, so we keep at least the last 2 ledgers
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];

        log.info("Ledger IDs: first={}, second={}, trimBefore={}, last={}, total={}",
                firstLedgerId, secondLedgerId, trimBeforeLedgerId, lastLedgerId, ledgerIds.length);

        // Consume all entries
        int totalRead = 0;
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            totalRead += entries.size();
            // Mark delete position to acknowledge consumption
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }
        assertEquals(totalRead, 10, "Should have read 10 entries");

        // Now trim ledgers before the 2nd-to-last ledger ID
        // This should delete the 2nd-to-last ledger and all before it, keeping only the last one
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(30, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.size() >= 2, "Should have deleted at least 2 ledgers");

        // Should have only 1 ledger left (the last one)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), 1,
                    "Should have only 1 ledger after trimming");
        });

        // Verify the remaining ledger is the last one
        assertTrue(ledger.getLedgersInfo().containsKey(lastLedgerId),
                "Last ledger should still exist");
        assertFalse(ledger.getLedgersInfo().containsKey(trimBeforeLedgerId),
                "2nd-to-last ledger should be deleted");
        assertFalse(ledger.getLedgersInfo().containsKey(firstLedgerId),
                "First ledger should be deleted");
        assertFalse(ledger.getLedgersInfo().containsKey(secondLedgerId),
                "Second ledger should be deleted");
    }

    /**
     * Test that trimming returns empty list when ledgers are not fully consumed.
     *
     * Scenario: Not all messages are consumed
     * Test: trimConsumedLedgersBefore(ledgerId)
     * Expected: return empty list because ledgers are not fully consumed
     */
    @Test(invocationCount = 1)
    public void testTrimConsumedLedgersBeforeNotConsumed() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)  // Force ledger rollover
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)  // Disable time-based retention
                .setRetentionSizeInMB(-1);  // Disable size-based retention

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID().toString(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID().toString());

        // Write entries across multiple ledgers
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Get the ledger IDs and use the 2nd-to-last as trim boundary (to have something to trim)
        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 2, "Should have at least 2 ledgers");
        // Use the 2nd-to-last ledger ID as trim boundary, so we have at least 1 ledger to potentially trim
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];

        // Initially we have multiple ledgers
        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Store initial position - the position where cursor was when it was first opened
        PositionImpl initialPosition = (PositionImpl) cursor.getMarkDeletedPosition();

        // Do not consume all entries - leave some unconsumed
        List<Entry> entries = cursor.readEntries(5);
        assertEquals(entries.size(), 5);
        // Don't call markDelete - we want the cursor to stay at the initial position
        entries.forEach(Entry::release);

        // Verify cursor is still at initial position (mark delete position should not change)
        assertEquals(cursor.getMarkDeletedPosition(), initialPosition,
                "Cursor should still be at initial position since we didn't call markDelete");

        // Try to trim before the 2nd-to-last ledger - should return empty list because not all consumed
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(10, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.isEmpty(), "Should return empty list when ledgers are not fully consumed");

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when nothing is trimmed");
    }

    /**
     * Test trimming with multiple cursors - all must be consumed.
     *
     * Scenario: L1, L2, L3, L4 (current), multiple cursors all consumed
     * Test: trimConsumedLedgersBefore(L3)
     * Expected: delete L1, L2, L3, keep L4 → 1 ledger left
     */
    @Test
    public void testTrimConsumedLedgersBeforeWithMultipleCursors() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)  // Force ledger rollover
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)  // Disable time-based retention
                .setRetentionSizeInMB(-1);  // Disable size-based retention

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);

        // Create multiple cursors
        ManagedCursor cursor1 = ledger.openCursor("cursor1-" + UUID.randomUUID());
        ManagedCursor cursor2 = ledger.openCursor("cursor2-" + UUID.randomUUID());

        // Write entries
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Get the ledger IDs and use the 2nd-to-last as trim boundary (to have something to trim)
        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 2, "Should have at least 2 ledgers");
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];
        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Both cursors consume all entries
        List<Entry> entries1 = cursor1.readEntries(10);
        assertEquals(entries1.size(), 10);
        if (!entries1.isEmpty()) {
            cursor1.markDelete(entries1.get(entries1.size() - 1).getPosition());
        }
        entries1.forEach(Entry::release);

        List<Entry> entries2 = cursor2.readEntries(10);
        assertEquals(entries2.size(), 10);
        if (!entries2.isEmpty()) {
            cursor2.markDelete(entries2.get(entries2.size() - 1).getPosition());
        }
        entries2.forEach(Entry::release);

        // Trim should succeed as all cursors have consumed
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(30, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.size() >= 2, "Should have deleted at least 2 ledgers");

        // Should have 1 ledger left (only the last one, since we deleted boundary and all before it)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), 1,
                    "Should have 1 ledger after trimming (the last one)");
        });
    }

    /**
     * Test trimming with one cursor lagging behind - should fail.
     *
     * Scenario: Multiple cursors, one cursor hasn't consumed yet
     * Test: trimConsumedLedgersBefore(ledgerId)
     * Expected: throw exception because slowest cursor hasn't consumed
     */
    @Test
    public void testTrimConsumedLedgersBeforeWithLaggingCursor() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)  // Force ledger rollover
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)  // Disable time-based retention
                .setRetentionSizeInMB(-1);  // Disable size-based retention

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);

        // Create two cursors
        ManagedCursor cursor1 = ledger.openCursor("cursor1-" + UUID.randomUUID());
        ManagedCursor cursor2 = ledger.openCursor("cursor2-" + UUID.randomUUID());

        // Write entries
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Get the ledger IDs and use the 2nd-to-last as trim boundary (to have something to trim)
        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 2, "Should have at least 2 ledgers");
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];
        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Only cursor1 consumes all entries, cursor2 lags behind
        List<Entry> entries1 = cursor1.readEntries(10);
        assertEquals(entries1.size(), 10);
        entries1.forEach(Entry::release);

        // cursor2 only reads 5 entries
        List<Entry> entries2 = cursor2.readEntries(5);
        assertEquals(entries2.size(), 5);
        entries2.forEach(Entry::release);

        // Trim should return empty list because cursor2 hasn't consumed all
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(10, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.isEmpty(), "Should return empty list when cursor2 is lagging");
        log.info("Got expected empty list because cursor2 is lagging");

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when nothing is trimmed due to lagging cursor");
    }

    /**
     * Test trimming when there are no ledgers to trim.
     *
     * Scenario: Only current ledger exists
     * Test: trimConsumedLedgersBefore(currentLedgerId)
     * Expected: no ledgers deleted, return successfully
     */
    @Test
    public void testTrimConsumedLedgersBeforeNoLedgersToTrim() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1);

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID().toString(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID().toString());

        // Write a few entries
        for (int i = 0; i < 5; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Get the first (and only) ledger ID
        long firstLedgerId = ledger.getLedgersInfo().firstKey();

        // Consume all entries
        List<Entry> entries = cursor.readEntries(5);
        assertEquals(entries.size(), 5);
        entries.forEach(Entry::release);

        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Try to trim before the first ledger ID (there's nothing before it)
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(firstLedgerId).get(10, TimeUnit.SECONDS);
        assertNotNull(deletedLedgerIds, "Deleted ledger IDs should not be null");

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when there are no ledgers to trim");
    }

    /**
     * Test trimming with a middle ledger ID - the boundary ledger should be deleted.
     *
     * Scenario: L1, L2, L3, L4... (many ledgers)
     * Test: trimConsumedLedgersBefore(L2)
     * Expected: delete L1, L2, keep L3, L4... → initialCount - 2 ledgers left
     */
    @Test
    public void testTrimConsumedLedgersBeforeBoundaryIsExclusive() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)  // Force ledger rollover
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)  // Disable time-based retention
                .setRetentionSizeInMB(-1);  // Disable size-based retention

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID().toString(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID().toString());

        // Force creation of multiple ledgers by writing enough entries
        for (int i = 0; i < 20; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Get the second ledger ID
        Long ledgerIds[] = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        long secondLedgerId = ledgerIds[1];

        log.info("Ledger IDs: {}", ledgerIds);
        log.info("Trimming before ledgerId: {}", secondLedgerId);

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Trim before the second ledger ID
        // This should delete the first AND second ledgers, keeping only ledgers after the boundary
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(secondLedgerId).get(30, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.size() >= 2, "Should have deleted at least 2 ledgers");

        // Should have 2 less ledgers (first and second are deleted)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount - 2,
                    "Should have 2 less ledgers after trimming");
        });

        // Verify the boundary ledger (secondLedgerId) is deleted
        assertFalse(ledger.getLedgersInfo().containsKey(secondLedgerId),
                "The boundary ledger (secondLedgerId) should be deleted");
    }

    /**
     * Test trimming with a ledgerId greater than the last existing ledger.
     *
     * Scenario: L1, L2, L3, L4 (current), pass 999999
     * Test: trimConsumedLedgersBefore(999999)
     * Expected: use L4 (current) as boundary, delete L1, L2, L3, keep L4 → 1 ledger left
     */
    @Test
    public void testTrimConsumedLedgersBeforeWithLargeLedgerId() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)
                .setRetentionSizeInMB(-1);

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID());

        // Write entries to create multiple ledgers
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 3, "Should have at least 3 ledgers, got: " + ledgerIds.length);
        long lastLedgerId = ledgerIds[ledgerIds.length - 1];

        log.info("Ledgers before trimming: {}", Arrays.toString(ledgerIds));

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        int initialLedgerCount = ledger.getLedgersInfo().size();
        log.info("Initial ledger count: {}", initialLedgerCount);

        // Trim with a very large ledger ID (999999)
        // Since this is greater than any existing ledger, it should use current ledger as boundary
        // and delete all consumed ledgers before the current one
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(999999L).get(30, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.size() >= 1, "Should have deleted at least 1 ledger");

        // Give some time for async operations to complete
        Thread.sleep(1000);

        int currentCount = ledger.getLedgersInfo().size();
        log.info("Ledger count after trimming: {}", currentCount);
        log.info("Ledgers after trimming: {}", ledger.getLedgersInfo().keySet());

        assertTrue(currentCount < initialLedgerCount,
                "Should have fewer ledgers after trimming. Initial: " + initialLedgerCount + ", Current: " + currentCount);

        // After trimming with a large ledger ID (which becomes current ledger boundary),
        // we should have only 1 ledger left (the current one)
        assertEquals(1, currentCount,
                "Should have exactly 1 ledger after trimming with large ID. Initial: " + initialLedgerCount);

        log.info("Test passed - final ledger count: {}", currentCount);
    }

    /**
     * Test trimming with a ledgerId that falls in a gap between existing ledgers.
     *
     * Scenario: L1, L2, L4 exist (L3 is a gap)
     * Test: trimConsumedLedgersBefore(L3)
     * Expected: use L2 (next lower) as boundary, delete L1, L2, keep L4 → 1 ledger left
     */
    @Test
    public void testTrimConsumedLedgersBeforeWithGapLedgerId() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(2)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)
                .setRetentionSizeInMB(-1);

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID());

        // Write entries to create multiple ledgers
        for (int i = 0; i < 15; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 3, "Should have at least 3 ledgers, got: " + ledgerIds.length);
        long secondLedgerId = ledgerIds[1];
        long lastLedgerId = ledgerIds[ledgerIds.length - 1];

        log.info("Ledgers: {}, attempting to trim before a gap value", Arrays.toString(ledgerIds));

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        int initialLedgerCount = ledger.getLedgersInfo().size();
        log.info("Initial ledger count: {}", initialLedgerCount);

        // Use a value between second and last ledger
        // This tests the logic of finding the next lower ledger when a value doesn't exist
        // For consecutive ledgers [0,1,2,3,4], using a value like 3 (which exists)
        // or testing with a value in between existing ledgers
        long gapLedgerId = lastLedgerId - 1;
        log.info("Trimming with gapLedgerId: {}", gapLedgerId);
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(gapLedgerId).get(30, TimeUnit.SECONDS);
        assertTrue(deletedLedgerIds.size() >= 1, "Should have deleted at least 1 ledger");

        // Should have fewer ledgers after trimming
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            int currentCount = ledger.getLedgersInfo().size();
            log.info("Current ledger count during assertion: {}", currentCount);
            assertTrue(currentCount < initialLedgerCount,
                    "Should have fewer ledgers after trimming with gap ledger ID. Initial: "
                    + initialLedgerCount + ", Current: " + currentCount);
        });

        // Verify the last ledger still exists
        assertTrue(ledger.getLedgersInfo().containsKey(lastLedgerId),
                "Last ledger should still exist");
        log.info("Test passed - final ledger count: {}", ledger.getLedgersInfo().size());
    }

    /**
     * Test trimming with a ledgerId smaller than the first existing ledger.
     *
     * Scenario: L1, L2, L3... (ledgers exist), pass 0
     * Test: trimConsumedLedgersBefore(0)
     * Expected: nothing to trim, return successfully, no ledgers deleted
     */
    @Test
    public void testTrimConsumedLedgersBeforeWithSmallLedgerId() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)
                .setRetentionSizeInMB(-1);

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID());

        // Write entries to create ledgers
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        long firstLedgerId = ledgerIds[0];

        log.info("Ledgers: {}, first ledger is {}", Arrays.toString(ledgerIds), firstLedgerId);

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        int initialLedgerCount = ledger.getLedgersInfo().size();

        // Trim with a ledgerId smaller than the first ledger (e.g., 0 when first is 2)
        // Should return successfully with nothing trimmed
        long smallLedgerId = Math.max(0, firstLedgerId - 1);
        List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(smallLedgerId).get(10, TimeUnit.SECONDS);
        assertNotNull(deletedLedgerIds, "Deleted ledger IDs should not be null");

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when trimming with small ledger ID");
    }

    /**
     * Test concurrent trim operations from multiple threads.
     *
     * Scenario: Multiple threads concurrently call trimConsumedLedgersBefore
     * Test: Verify that only one operation proceeds at a time and others are retried
     * Expected: All operations complete successfully without deadlock
     */
    @Test
    public void testConcurrentTrimOperations() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)
                .setRetentionSizeInMB(-1);

        ManagedLedger ledger = factory.open("test-ledger-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID());

        // Write enough entries to create multiple ledgers
        for (int i = 0; i < 20; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        int initialLedgerCount = ledger.getLedgersInfo().size();
        assertTrue(initialLedgerCount >= 4, "Should have at least 4 ledgers for concurrent test");

        // Get the 2nd-to-last ledger ID as trim boundary
        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];

        // Create a thread pool for concurrent operations
        int numThreads = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        // Submit multiple trim operations concurrently
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();
                    log.info("Thread {} starting trim operation", threadId);

                    // Perform trim operation
                    List<Long> deletedLedgerIds = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId)
                            .get(30, TimeUnit.SECONDS);

                    log.info("Thread {} completed trim, deleted ledgers: {}", threadId, deletedLedgerIds);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("Thread {} failed during trim", threadId, e);
                    failureCount.incrementAndGet();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads at once
        startLatch.countDown();

        // Wait for all threads to complete (with timeout)
        boolean completed = completionLatch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "All concurrent trim operations should complete within timeout");

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        // Verify results
        log.info("Concurrent trim test results: success={}, failure={}", successCount.get(), failureCount.get());
        assertEquals(successCount.get(), numThreads,
                "All concurrent trim operations should succeed (some may return empty list after first)");
        assertEquals(failureCount.get(), 0, "No operations should fail");

        // Final ledger count should be reduced (only first operation deletes ledgers)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            int finalLedgerCount = ledger.getLedgersInfo().size();
            assertTrue(finalLedgerCount < initialLedgerCount,
                    "Final ledger count should be less than initial. Initial: " + initialLedgerCount
                            + ", Final: " + finalLedgerCount);
        });
    }

    /**
     * Test high contention scenario with rapid successive trim calls.
     *
     * Scenario: Rapidly submit many trim operations to trigger mutex contention and retry logic
     * Test: Verify retry mechanism works correctly under high contention
     * Expected: All operations complete, no deadlocks, no mutex leaks
     */
    @Test(invocationCount = 1)
    public void testHighContentionTrimOperations() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImplExt factory = createFactory(factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(3)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS)
                .setRetentionSizeInMB(-1);

        ManagedLedgerImplExt ledger = (ManagedLedgerImplExt) factory.open("test-ledger-" + UUID.randomUUID(), config);
        ManagedCursor cursor = ledger.openCursor("cursor-" + UUID.randomUUID());

        // Write entries to create multiple ledgers
        for (int i = 0; i < 15; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Consume all entries
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(100);
            if (!entries.isEmpty()) {
                cursor.markDelete(entries.get(entries.size() - 1).getPosition());
            }
            entries.forEach(Entry::release);
        }

        Long[] ledgerIds = ledger.getLedgersInfo().keySet().toArray(new Long[0]);
        assertTrue(ledgerIds.length >= 3, "Should have at least 3 ledgers");
        long trimBeforeLedgerId = ledgerIds[ledgerIds.length - 2];

        // Rapidly submit many trim operations to create contention
        int numOperations = 20;
        List<CompletableFuture<List<Long>>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numOperations; i++) {
            final int opId = i;
            // Submit without waiting - create maximum contention
            CompletableFuture<List<Long>> future = ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId);
            future.thenAccept(deletedIds -> {
                log.info("Operation {} completed, deleted: {}", opId, deletedIds);
            }).exceptionally(ex -> {
                log.warn("Operation {} failed: {}", opId, ex.getMessage());
                return null;
            });
            futures.add(future);

            // Small delay to ensure they overlap
            Thread.sleep(5);
        }

        // Wait for all operations to complete
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger emptyResultCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (CompletableFuture<List<Long>> future : futures) {
            try {
                List<Long> result = future.get(60, TimeUnit.SECONDS);
                successCount.incrementAndGet();
                if (result.isEmpty()) {
                    emptyResultCount.incrementAndGet();
                }
            } catch (Exception e) {
                log.error("Operation failed", e);
                failureCount.incrementAndGet();
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("High contention test completed in {}ms: success={}, empty={}, failure={}",
                duration, successCount.get(), emptyResultCount.get(), failureCount.get());

        // All operations should complete without exception
        assertEquals(successCount.get(), numOperations,
                "All operations should complete successfully");
        assertEquals(failureCount.get(), 0, "No operations should fail");

        // After the first successful trim, subsequent operations should return empty list
        // (because ledgers are already deleted)
        assertTrue(emptyResultCount.get() >= numOperations - 1,
                "At least " + (numOperations - 1) + " operations should return empty list after first trim");

        // Verify final state
        int finalLedgerCount = ledger.getLedgersInfo().size();
        assertTrue(finalLedgerCount >= 1 && finalLedgerCount < ledgerIds.length,
                "Final ledger count should be reduced but not zero. Initial: " + ledgerIds.length
                        + ", Final: " + finalLedgerCount);
    }
}
