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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Test;

/**
 * Ordering guarantees of {@link CursorCheckpointPersistence}: the per-msg-ledger checkpoint
 * position index must never regress, otherwise a later AckStateRef would point at an older
 * (smaller) ack bitmap and acks recorded in the newer checkpoint would be lost on recovery.
 */
public class CursorCheckpointPersistenceOrderingTest {

    private CursorCheckpointPersistence newInstance() {
        return new CursorCheckpointPersistence(null, new ReentrantReadWriteLock(),
                null, null, new byte[0], true);
    }

    @Test
    public void testRecordCheckpointPosMonotonic() {
        CursorCheckpointPersistence persistence = newInstance();
        assertNull(persistence.checkpointPosOf(5));

        // First record wins by absence.
        persistence.recordCheckpointPos(5, pos(1, 10));
        assertEquals(persistence.checkpointPosOf(5), pos(1, 10));

        // Newer position advances the index.
        persistence.recordCheckpointPos(5, pos(1, 20));
        assertEquals(persistence.checkpointPosOf(5), pos(1, 20));

        // A stale completion (out-of-order append callback, same cursor ledger) must not regress.
        persistence.recordCheckpointPos(5, pos(1, 15));
        assertEquals(persistence.checkpointPosOf(5), pos(1, 20));

        // A position from an older cursor ledger (after rollover replay) must not regress either.
        persistence.recordCheckpointPos(5, pos(0, 999));
        assertEquals(persistence.checkpointPosOf(5), pos(1, 20));

        // Ledger ids are independent entries.
        persistence.recordCheckpointPos(7, pos(1, 3));
        assertEquals(persistence.checkpointPosOf(7), pos(1, 3));
        assertEquals(persistence.checkpointPosOf(5), pos(1, 20));
    }

    private static Position pos(long ledgerId, long entryId) {
        return PositionFactory.create(ledgerId, entryId);
    }
}
