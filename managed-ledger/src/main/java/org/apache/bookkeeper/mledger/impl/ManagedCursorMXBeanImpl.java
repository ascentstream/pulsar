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

import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;

public class ManagedCursorMXBeanImpl implements ManagedCursorMXBean {

    private final LongAdder persistLedgeSucceed = new LongAdder();
    private final LongAdder persistLedgeFailed = new LongAdder();

    private final LongAdder persistZookeeperSucceed = new LongAdder();
    private final LongAdder persistZookeeperFailed = new LongAdder();

    private final LongAdder writeCursorLedgerSize = new LongAdder();
    private final LongAdder writeCursorLedgerLogicalSize = new LongAdder();
    private final LongAdder readCursorLedgerSize = new LongAdder();

    private final LongAdder ackCount = new LongAdder();
    private final LongAdder ackLatencyTotalMillis = new LongAdder();

    private final LongAdder persistCount = new LongAdder();
    private final LongAdder persistLatencyTotalMillis = new LongAdder();

    private final LongAdder recoverCount = new LongAdder();
    private final LongAdder recoverSucceed = new LongAdder();
    private final LongAdder recoverErrors = new LongAdder();
    private final LongAdder recoverLatencyTotalMillis = new LongAdder();

    private final ManagedCursor managedCursor;

    public ManagedCursorMXBeanImpl(ManagedCursor managedCursor) {
        this.managedCursor = managedCursor;
    }

    @Override
    public String getName() {
        return this.managedCursor.getName();
    }

    @Override
    public String getLedgerName() {
        return this.managedCursor.getManagedLedger().getName();
    }

    @Override
    public void persistToLedger(boolean success) {
        if (success) {
            persistLedgeSucceed.increment();
        } else {
            persistLedgeFailed.increment();
        }
    }

    @Override
    public void persistToZookeeper(boolean success) {
        if (success) {
            persistZookeeperSucceed.increment();
        } else {
            persistZookeeperFailed.increment();
        }
    }

    @Override
    public long getPersistLedgerSucceed() {
        return persistLedgeSucceed.longValue();
    }

    @Override
    public long getPersistLedgerErrors() {
        return persistLedgeFailed.longValue();
    }

    @Override
    public long getPersistZookeeperSucceed() {
        return persistZookeeperSucceed.longValue();
    }

    @Override
    public long getPersistZookeeperErrors() {
        return persistZookeeperFailed.longValue();
    }

    @Override
    public void addWriteCursorLedgerSize(final long size) {
        writeCursorLedgerSize.add(
                size * managedCursor.getManagedLedger().getConfig().getWriteQuorumSize());
        writeCursorLedgerLogicalSize.add(size);
    }

    @Override
    public void addReadCursorLedgerSize(final long size) {
        readCursorLedgerSize.add(size);
    }

    @Override
    public long getWriteCursorLedgerSize() {
        return writeCursorLedgerSize.longValue();
    }

    @Override
    public long getWriteCursorLedgerLogicalSize() {
        return writeCursorLedgerLogicalSize.longValue();
    }

    @Override
    public long getReadCursorLedgerSize() {
        return readCursorLedgerSize.longValue();
    }

    @Override
    public void recordAck(long latencyMillis) {
        ackCount.increment();
        ackLatencyTotalMillis.add(latencyMillis);
    }

    @Override
    public long getAckCount() {
        return ackCount.longValue();
    }

    @Override
    public double getAckLatencyAvgMillis() {
        long count = ackCount.longValue();
        return count == 0 ? 0 : ackLatencyTotalMillis.longValue() / (double) count;
    }

    @Override
    public void recordPersist(long latencyMillis) {
        persistCount.increment();
        persistLatencyTotalMillis.add(latencyMillis);
    }

    @Override
    public long getPersistCount() {
        return persistCount.longValue();
    }

    @Override
    public double getPersistLatencyAvgMillis() {
        long count = persistCount.longValue();
        return count == 0 ? 0 : persistLatencyTotalMillis.longValue() / (double) count;
    }

    @Override
    public void recordRecover(long latencyMillis, boolean success) {
        recoverCount.increment();
        recoverLatencyTotalMillis.add(latencyMillis);
        if (success) {
            recoverSucceed.increment();
        } else {
            recoverErrors.increment();
        }
    }

    @Override
    public long getRecoverCount() {
        return recoverCount.longValue();
    }

    @Override
    public long getRecoverSucceed() {
        return recoverSucceed.longValue();
    }

    @Override
    public long getRecoverErrors() {
        return recoverErrors.longValue();
    }

    @Override
    public double getRecoverLatencyAvgMillis() {
        long count = recoverCount.longValue();
        return count == 0 ? 0 : recoverLatencyTotalMillis.longValue() / (double) count;
    }
}
