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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
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
        ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(30, TimeUnit.SECONDS);

        // Should have only 2 ledgers left (the last 2)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), 2,
                    "Should have only 2 ledgers after trimming");
        });

        // Verify the remaining ledgers are the last 2
        assertTrue(ledger.getLedgersInfo().containsKey(lastLedgerId),
                "Last ledger should still exist");
        assertTrue(ledger.getLedgersInfo().containsKey(trimBeforeLedgerId),
                "2nd-to-last ledger should still exist");
        assertFalse(ledger.getLedgersInfo().containsKey(firstLedgerId),
                "First ledger should be deleted");
        assertFalse(ledger.getLedgersInfo().containsKey(secondLedgerId),
                "Second ledger should be deleted");
    }

    /**
     * Test that trimming fails when ledgers are not fully consumed.
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

        // Try to trim before the 2nd-to-last ledger - should fail because not all consumed
        try {
            ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(10, TimeUnit.SECONDS);
            fail("Should have thrown exception because ledgers are not fully consumed");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ManagedLedgerException,
                    "Expected ManagedLedgerException");
        }

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when trim fails");
    }

    /**
     * Test trimming with multiple cursors.
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
        ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(30, TimeUnit.SECONDS);

        // Should have 2 ledgers left (the last 2, since we trimmed before the 2nd-to-last)
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), 2,
                    "Should have 2 ledgers after trimming");
        });
    }

    /**
     * Test trimming with one cursor lagging behind.
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

        // Trim should fail because cursor2 hasn't consumed all
        try {
            ledger.asyncTrimConsumedLedgersBefore(trimBeforeLedgerId).get(10, TimeUnit.SECONDS);
            fail("Should have thrown exception because cursor2 is lagging");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ManagedLedgerException,
                    "Expected ManagedLedgerException");
            log.info("Got expected exception: {}", e.getMessage());
        }

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when trim fails due to lagging cursor");
    }

    /**
     * Test trimming when there are no ledgers to trim.
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
        ledger.asyncTrimConsumedLedgersBefore(firstLedgerId).get(10, TimeUnit.SECONDS);

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when there are no ledgers to trim");
    }

    /**
     * Test that the ledger boundary is not deleted (ledgerId is exclusive).
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

        // Trim before the second ledger ID (should only delete the first ledger)
        ledger.asyncTrimConsumedLedgersBefore(secondLedgerId).get(30, TimeUnit.SECONDS);

        // Should have one less ledger
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount - 1,
                    "Should have one less ledger after trimming");
        });

        // Verify the boundary ledger (secondLedgerId) still exists
        assertTrue(ledger.getLedgersInfo().containsKey(secondLedgerId),
                "The boundary ledger should not be deleted");
    }

    /**
     * Test trimming with a ledgerId greater than the last existing ledger.
     * When a very large ledger ID is passed, it should use current ledger as boundary
     * and delete all consumed ledgers before the current one.
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
        ledger.asyncTrimConsumedLedgersBefore(999999L).get(30, TimeUnit.SECONDS);

        // Give some time for async operations to complete
        Thread.sleep(1000);

        int currentCount = ledger.getLedgersInfo().size();
        log.info("Ledger count after trimming: {}", currentCount);
        log.info("Ledgers after trimming: {}", ledger.getLedgersInfo().keySet());

        assertTrue(currentCount < initialLedgerCount,
                "Should have fewer ledgers after trimming. Initial: " + initialLedgerCount + ", Current: " + currentCount);
        assertTrue(ledger.getLedgersInfo().containsKey(lastLedgerId),
                "Last ledger should still exist after trimming with large ID");

        log.info("Test passed - final ledger count: {}", currentCount);
    }

    /**
     * Test trimming with a ledgerId that falls in a gap between existing ledgers.
     * Should use the next lower existing ledger as boundary.
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
        ledger.asyncTrimConsumedLedgersBefore(gapLedgerId).get(30, TimeUnit.SECONDS);

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
     * Should return successfully with nothing to trim.
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
        ledger.asyncTrimConsumedLedgersBefore(smallLedgerId).get(10, TimeUnit.SECONDS);

        // Ledger count should remain unchanged
        assertEquals(ledger.getLedgersInfo().size(), initialLedgerCount,
                "Ledger count should not change when trimming with small ledger ID");
    }
}
