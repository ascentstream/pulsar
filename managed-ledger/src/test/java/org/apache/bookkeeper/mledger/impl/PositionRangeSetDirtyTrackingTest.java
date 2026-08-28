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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.testng.annotations.Test;

/**
 * Regression tests for {@link PositionRangeSet} dirty tracking.
 *
 * <p>These tests cover the three markDirty fixes:
 * <ol>
 *   <li>Same-ledger acks (upperLedgerId == lowerLedgerId) now mark dirty — original code
 *       had {@code upperLedgerId <= lowerLedgerId} which silently skipped them.</li>
 *   <li>Range end is inclusive — original {@code add(L+1, U+1)} missed the upper ledger;
 *       fixed to {@code add(L+1, U+2)}.</li>
 *   <li>snapshotAndClearDirtyLedgers correctly maps dirty bits back to raw ledger IDs.</li>
 * </ol>
 */
public class PositionRangeSetDirtyTrackingTest {

    private static final LongPairConsumer<Position> CONVERTER =
            (ledgerId, entryId) -> PositionFactory.create(ledgerId, entryId);

    @Test
    public void testSameLedgerAckMarksDirty() {
        // Regression: same-ledger ack (the most common case) was silently skipped by
        // the original markDirty when upperLedgerId == lowerLedgerId.
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 5);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactly(100L);
    }

    @Test
    public void testCrossLedgerRangeMarksAllInclusive() {
        // Regression: markDirty(L1, L2) should mark L1 through L2 INCLUSIVE.
        // Original add(L1+1, L2+1) [half-open] missed L2.
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 103, 0);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactlyInAnyOrder(100L, 101L, 102L, 103L);
    }

    @Test
    public void testDirtyClearedAfterSnapshot() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 5);
        set.snapshotAndClearDirtyLedgers();
        // After snapshot, dirty should be empty
        Set<Long> dirty2 = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty2).isEmpty();
    }

    @Test
    public void testRestoreDirtyLedgers() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 5);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        // Restore dirty ledgers (simulates failed persist)
        set.restoreDirtyLedgers(dirty);
        // Next snapshot should have them again
        Set<Long> dirty2 = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty2).containsExactly(100L);
    }

    @Test
    public void testMultipleLedgersIndividuallyDirty() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 3);
        set.addOpenClosed(200, 0, 200, 7);
        set.addOpenClosed(300, 0, 300, 1);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactlyInAnyOrder(100L, 200L, 300L);
    }

    @Test
    public void testBitmapOfReturnsNullForEmptyLedger() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        assertThat(set.bitmapOf(999)).isNull();
    }

    @Test
    public void testBitmapOfReturnsBytesForActiveLedger() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 5);
        byte[] bytes = set.bitmapOf(100);
        assertThat(bytes).isNotNull();
        assertThat(bytes.length).isGreaterThan(0);
    }

    @Test
    public void testForEachActiveLedger() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 100, 1);
        set.addOpenClosed(200, 0, 200, 1);
        Set<Long> active = new java.util.HashSet<>();
        set.forEachActiveLedger(active::add);
        assertThat(active).containsExactlyInAnyOrder(100L, 200L);
    }

    @Test
    public void testDirtyDisabledWhenMultiEntryOff() {
        // When enableMultiEntry is false, dirty tracking is a no-op.
        PositionRangeSet set = new PositionRangeSet(CONVERTER, false);
        set.addOpenClosed(100, 0, 100, 5);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).isEmpty();
    }

    @Test
    public void testSameLedgerMarkDirty() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(0, -1, 0, 0);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactly(0L);
    }

    @Test
    public void testCrossLedgerMarkDirtyUpperBound() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(100, 0, 103, 0);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactlyInAnyOrder(100L, 101L, 102L, 103L);
    }

    @Test
    public void testRestoreAfterSnapshot() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(50, 0, 50, 1);
        set.addOpenClosed(60, 0, 60, 1);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactlyInAnyOrder(50L, 60L);
        assertThat(set.snapshotAndClearDirtyLedgers()).isEmpty();
        set.restoreDirtyLedgers(dirty);
        assertThat(set.snapshotAndClearDirtyLedgers()).containsExactlyInAnyOrder(50L, 60L);
    }

    @Test
    public void testRemoveAtMostClearsDirty() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        set.addOpenClosed(10, 0, 20, 0);
        set.removeAtMost(15, 0);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        for (long id : dirty) {
            assertThat(id).isGreaterThan(14);
        }
    }

    @Test
    public void testEmptySnapshot() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        assertThat(set.snapshotAndClearDirtyLedgers()).isEmpty();
    }

    @Test
    public void testMarkDirtyAtMaxValueBoundary() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        long near = Integer.MAX_VALUE - 1;
        set.addOpenClosed(near, 0, near, 1);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactly(near);
    }

    @Test
    public void testMarkDirtyUpperAtMaxValue() {
        PositionRangeSet set = new PositionRangeSet(CONVERTER, true);
        long near = Integer.MAX_VALUE - 1;
        set.addOpenClosed(near - 1, 0, near, 0);
        Set<Long> dirty = set.snapshotAndClearDirtyLedgers();
        assertThat(dirty).containsExactlyInAnyOrder(near - 1, near);
    }
}
