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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import org.testng.annotations.Test;

public class TripleLongPriorityQueueTest {

    @Test
    public void testQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int num = 1000;

        for (int i = num; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), num);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= num; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), num - i);
        }

        pq.close();
    }

    @Test
    public void testLargeQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int num = 3_000_000;

        for (int i = num; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), num);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= num; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), num - i);
        }

        pq.clear();
        pq.close();
    }


    @Test
    public void testCheckForEmpty() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        try {
            pq.peekN1();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.peekN2();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.peekN3();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.pop();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        pq.close();
    }

    @Test
    public void testCompareWithSamePrefix() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        pq.add(10, 20, 30);
        pq.add(20, 10, 10);
        pq.add(10, 20, 10);
        pq.add(10, 30, 10);
        pq.add(10, 20, 5);

        assertEquals(pq.size(), 5);

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 5);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 30);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 30);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.peekN1(), 20);
        assertEquals(pq.peekN2(), 10);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        pq.close();
    }

    @Test
    public void testShrink() throws Exception {
        int initialCapacity = 20;
        int tupleSize = 3 * 8;
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue(initialCapacity, 0.5f);
        pq.add(0, 0, 0);
        assertEquals(pq.size(), 1);
        assertEquals(pq.bytesCapacity(), initialCapacity * tupleSize);

        // Scale out to capacity * 2
        triggerScaleOut(initialCapacity, pq);
        int scaleCapacity = initialCapacity * 2;
        assertEquals(pq.bytesCapacity(), scaleCapacity * tupleSize);
        // Trigger shrinking
        for (int i = 0; i < initialCapacity / 2 + 2; i++) {
             pq.pop();
        }
        int capacity = scaleCapacity - (int) ((scaleCapacity) * 0.5f * 0.9f);
        assertTrue(pq.bytesCapacity() < scaleCapacity * tupleSize);
        // Scale out to capacity * 2
        triggerScaleOut(initialCapacity, pq);
        scaleCapacity = capacity * 2;
        // Trigger shrinking
        pq.clear();
        capacity = scaleCapacity - (int) (scaleCapacity * 0.5f * 0.9f);
    }

    private void triggerScaleOut(int initialCapacity, TripleLongPriorityQueue pq) {
        for (long i = 0; i < initialCapacity + 1; i++) {
            pq.add(i, i, i);
        }
    }

    @Test
    public void disableAutoShrinkHoldsCapacityDuringDrain() {
        int initialCapacity = 20;
        TripleLongPriorityQueue pq =
                new TripleLongPriorityQueue(initialCapacity, 0.5f, true);
        // Grow well beyond the initial backing array.
        for (int i = 0; i < initialCapacity + 10; i++) {
            pq.add(i, i, i);
        }
        long capacityAfterGrow = pq.bytesCapacity();
        assertTrue(capacityAfterGrow > initialCapacity * 3L * Byte.BYTES,
                "expected the backing array to have grown beyond the initial capacity");

        // Drain almost everything (leave 1 to stay off the count==0 fast path in pop).
        while (pq.size() > 1) {
            pq.pop();
        }
        assertEquals(pq.size(), 1L);
        assertTrue(pq.bytesCapacity() == capacityAfterGrow,
                "disableAutoShrink=true must keep the backing array unchanged on pop");
        pq.close();
    }

    @Test
    public void autoShrinkReclaimsDuringDrain() {
        int initialCapacity = 20;
        // Default constructor path -> auto shrink on (disableAutoShrink = false).
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue(initialCapacity, 0.5f);
        for (int i = 0; i < initialCapacity + 10; i++) {
            pq.add(i, i, i);
        }
        long capacityAfterGrow = pq.bytesCapacity();
        while (pq.size() > 1) {
            pq.pop();
        }
        assertTrue(pq.bytesCapacity() < capacityAfterGrow,
                "auto-shrink (default) must reclaim backing memory when draining below threshold");
        pq.close();
    }

    @Test
    public void manualShrinkCapacityReclaimsWhenAutoDisabled() {
        int initialCapacity = 20;
        TripleLongPriorityQueue pq =
                new TripleLongPriorityQueue(initialCapacity, 0.5f, true);
        for (int i = 0; i < initialCapacity + 10; i++) {
            pq.add(i, i, i);
        }
        while (pq.size() > 1) {
            pq.pop();
        }
        long heldCapacity = pq.bytesCapacity();
        // Caller-driven reclaim: shrinkCapacity() is public so opt-out callers can still trim.
        pq.shrinkCapacity();
        assertTrue(pq.bytesCapacity() < heldCapacity,
                "manual shrinkCapacity() must reclaim even when auto-shrink is disabled");
        pq.close();
    }

    @Test
    public void clearShrinksRegardlessOfDisableAutoShrink() {
        int initialCapacity = 20;
        TripleLongPriorityQueue pq =
                new TripleLongPriorityQueue(initialCapacity, 0.5f, true);
        for (int i = 0; i < initialCapacity + 10; i++) {
            pq.add(i, i, i);
        }
        long capacityAfterGrow = pq.bytesCapacity();
        pq.clear();
        assertEquals(pq.size(), 0L);
        assertTrue(pq.bytesCapacity() < capacityAfterGrow,
                "clear() must reclaim memory regardless of disableAutoShrink");
        pq.close();
    }

    @Test
    public void disableAutoShrinkPreservesOrderingAndReusesCapacityAcrossCycles() {
        int n = 2000;
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue(16, 0.5f, true);

        // Cycle 1: fill (descending n1) then drain — must come out ascending.
        for (int i = 0; i < n; i++) {
            pq.add(n - i, i / 10L, i);
        }
        long capacityAfterCycle1 = pq.bytesCapacity();
        assertDrainedInAscendingN1Order(pq, n);

        // After a full drain the queue is empty; with disableAutoShrink the backing array is kept,
        // so refilling the same number of tuples must reuse it (no re-growth).
        for (int i = 0; i < n; i++) {
            pq.add(n - i, (n + i) / 10L, n + i);
        }
        assertTrue(pq.bytesCapacity() == capacityAfterCycle1,
                "refill after a full drain must reuse the retained backing array, not re-grow");
        assertDrainedInAscendingN1Order(pq, n);
        pq.close();
    }

    private static void assertDrainedInAscendingN1Order(TripleLongPriorityQueue pq, int expectedCount) {
        long prev = Long.MIN_VALUE;
        int count = 0;
        while (!pq.isEmpty()) {
            long n1 = pq.peekN1();
            assertTrue(n1 >= prev, "min-heap ordering broken on drain: " + prev + " -> " + n1);
            prev = n1;
            pq.pop();
            count++;
        }
        assertEquals(count, expectedCount);
    }

    @Test
    public void testDifferentialRandomPriorityQueue() {
        Comparator<long[]> cmp = Comparator.comparingLong((long[] t) -> t[0])
                .thenComparingLong(t -> t[1])
                .thenComparingLong(t -> t[2]);

        for (int trial = 0; trial < 10; trial++) {
            Random rng = new Random(42 + trial);
            PriorityQueue<long[]> oracle = new PriorityQueue<>(cmp);
            try (TripleLongPriorityQueue pq = new TripleLongPriorityQueue()) {
                int ops = 10_000 + rng.nextInt(10_000);
                for (int i = 0; i < ops; i++) {
                    boolean doAdd = oracle.isEmpty() || rng.nextBoolean();
                    if (doAdd) {
                        // ~10% chance of same-prefix (small n1 range) to exercise tie-breaking
                        // note: use nextInt instead of RandomGenerator.nextLong(long) because
                        // this module is compiled with the client compiler release level
                        long n1 = rng.nextInt(100) < 10 ? rng.nextInt(20) : rng.nextInt(1_000_000);
                        long n2 = rng.nextInt(100);
                        long n3 = rng.nextInt(1_000_000);
                        oracle.add(new long[]{n1, n2, n3});
                        pq.add(n1, n2, n3);
                    } else {
                        long[] expected = oracle.poll();
                        assertNotNull(expected);
                        assertEquals(pq.peekN1(), expected[0], "n1 mismatch at op " + i);
                        assertEquals(pq.peekN2(), expected[1], "n2 mismatch at op " + i);
                        assertEquals(pq.peekN3(), expected[2], "n3 mismatch at op " + i);
                        pq.pop();
                    }
                    assertEquals(pq.size(), oracle.size(), "size mismatch at op " + i);
                }
                // drain remaining
                while (!oracle.isEmpty()) {
                    long[] expected = oracle.poll();
                    assertEquals(pq.peekN1(), expected[0]);
                    assertEquals(pq.peekN2(), expected[1]);
                    assertEquals(pq.peekN3(), expected[2]);
                    pq.pop();
                }
                assertTrue(pq.isEmpty());
            }
        }
    }
}
