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

import com.google.common.collect.Streams;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.opentelemetry.Constants;

public class OpenTelemetryManagedCursorStats implements AutoCloseable {

    // Replaces ['pulsar_ml_cursor_persistLedgerSucceed', 'pulsar_ml_cursor_persistLedgerErrors']
    public static final String PERSIST_OPERATION_COUNTER = "pulsar.broker.managed_ledger.persist.operation.count";
    private final ObservableLongMeasurement persistOperationCounter;

    // Replaces ['pulsar_ml_cursor_persistZookeeperSucceed', 'pulsar_ml_cursor_persistZookeeperErrors']
    public static final String PERSIST_OPERATION_METADATA_STORE_COUNTER =
            "pulsar.broker.managed_ledger.persist.mds.operation.count";
    private final ObservableLongMeasurement persistOperationMetadataStoreCounter;

    // Replaces pulsar_ml_cursor_nonContiguousDeletedMessagesRange
    public static final String NON_CONTIGUOUS_MESSAGE_RANGE_COUNTER =
            "pulsar.broker.managed_ledger.message_range.count";
    private final ObservableLongMeasurement nonContiguousMessageRangeCounter;

    // Replaces pulsar_ml_cursor_writeLedgerSize
    public static final String OUTGOING_BYTE_COUNTER = "pulsar.broker.managed_ledger.cursor.outgoing.size";
    private final ObservableLongMeasurement outgoingByteCounter;

    // Replaces pulsar_ml_cursor_writeLedgerLogicalSize
    public static final String OUTGOING_BYTE_LOGICAL_COUNTER =
            "pulsar.broker.managed_ledger.cursor.outgoing.logical.size";
    private final ObservableLongMeasurement outgoingByteLogicalCounter;

    // Replaces pulsar_ml_cursor_readLedgerSize
    public static final String INCOMING_BYTE_COUNTER = "pulsar.broker.managed_ledger.cursor.incoming.size";
    private final ObservableLongMeasurement incomingByteCounter;

    // Broker-level counters incremented when cursor persistence silently truncates ack state.
    // See managedLedgerMaxUnackedRangesToPersist and managedLedgerMaxBatchDeletedIndexToPersist.
    public static final String PERSIST_UNACKED_RANGES_TRUNCATED =
            "pulsar.broker.managed_ledger.cursor.persist.unacked_ranges.truncated";
    private final LongCounter persistUnackedRangesTruncated;

    public static final String PERSIST_BATCH_DELETED_INDEXES_TRUNCATED =
            "pulsar.broker.managed_ledger.cursor.persist.batch_deleted_indexes.truncated";
    private final LongCounter persistBatchDeletedIndexesTruncated;

    // Replaces ['brk_ml_cursor_ackCount']
    public static final String ACK_OPERATION_COUNTER =
            "pulsar.broker.managed_ledger.cursor.ack.operation.count";
    private final ObservableLongMeasurement ackOperationCounter;

    // Replaces ['brk_ml_cursor_ackLatencyAvgMs']
    public static final String ACK_LATENCY = "pulsar.broker.managed_ledger.cursor.ack.latency";
    private final DoubleHistogram ackLatency;

    // Replaces ['brk_ml_cursor_persistLatencyAvgMs']; the count is already covered by
    // PERSIST_OPERATION_COUNTER.
    public static final String PERSIST_LATENCY = "pulsar.broker.managed_ledger.cursor.persist.latency";
    private final DoubleHistogram persistLatency;

    // Replaces ['brk_ml_cursor_recoverCount', 'brk_ml_cursor_recoverSucceed',
    // 'brk_ml_cursor_recoverErrors'].
    public static final String RECOVER_OPERATION_COUNTER =
            "pulsar.broker.managed_ledger.cursor.recover.operation.count";
    private final ObservableLongMeasurement recoverOperationCounter;

    // Replaces ['brk_ml_cursor_recoverLatencyAvgMs']
    public static final String RECOVER_LATENCY = "pulsar.broker.managed_ledger.cursor.recover.latency";
    private final DoubleHistogram recoverLatency;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedCursorStats(OpenTelemetry openTelemetry, ManagedLedgerFactoryImpl factory) {
        var meter = openTelemetry.getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);

        persistOperationCounter = meter
                .counterBuilder(PERSIST_OPERATION_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of acknowledgment operations on the ledger.")
                .buildObserver();

        persistOperationMetadataStoreCounter = meter
                .counterBuilder(PERSIST_OPERATION_METADATA_STORE_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of acknowledgment operations in the metadata store.")
                .buildObserver();

        nonContiguousMessageRangeCounter = meter
                .upDownCounterBuilder(NON_CONTIGUOUS_MESSAGE_RANGE_COUNTER)
                .setUnit("{range}")
                .setDescription("The number of non-contiguous deleted messages ranges.")
                .buildObserver();

        outgoingByteCounter = meter
                .counterBuilder(OUTGOING_BYTE_COUNTER)
                .setUnit("{By}")
                .setDescription("The total amount of data written to the ledger.")
                .buildObserver();

        outgoingByteLogicalCounter = meter
                .counterBuilder(OUTGOING_BYTE_LOGICAL_COUNTER)
                .setUnit("{By}")
                .setDescription("The total amount of data written to the ledger, not including replicas.")
                .buildObserver();

        incomingByteCounter = meter
                .counterBuilder(INCOMING_BYTE_COUNTER)
                .setUnit("{By}")
                .setDescription("The total amount of data read from the ledger.")
                .buildObserver();

        persistUnackedRangesTruncated = meter
                .counterBuilder(PERSIST_UNACKED_RANGES_TRUNCATED)
                .setUnit("{truncation}")
                .setDescription("The number of times a cursor exceeded"
                        + " managedLedgerMaxUnackedRangesToPersist, causing ack state to be truncated"
                        + " at persistence. Ack state beyond the limit is lost on broker restart.")
                .build();

        persistBatchDeletedIndexesTruncated = meter
                .counterBuilder(PERSIST_BATCH_DELETED_INDEXES_TRUNCATED)
                .setUnit("{truncation}")
                .setDescription("The number of times a cursor exceeded"
                        + " managedLedgerMaxBatchDeletedIndexToPersist, causing batch deleted index state"
                        + " to be truncated at persistence. State beyond the limit is lost on broker restart.")
                .build();

        ackOperationCounter = meter
                .counterBuilder(ACK_OPERATION_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of cursor acknowledgment operations.")
                .buildObserver();

        ackLatency = meter.histogramBuilder(ACK_LATENCY)
                .setUnit("s")
                .setDescription("Cursor acknowledgment operation latency.")
                .build();

        persistLatency = meter.histogramBuilder(PERSIST_LATENCY)
                .setUnit("s")
                .setDescription("Cursor persist latency: the checkpoint write to the cursor ledger.")
                .build();

        recoverOperationCounter = meter
                .counterBuilder(RECOVER_OPERATION_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of cursor recovery operations.")
                .buildObserver();

        recoverLatency = meter.histogramBuilder(RECOVER_LATENCY)
                .setUnit("s")
                .setDescription("Cursor recovery latency.")
                .build();

        batchCallback = meter.batchCallback(() -> factory.getManagedLedgers()
                        .values()
                        .stream()
                        .map(ManagedLedger::getCursors)
                        .flatMap(Streams::stream)
                        .forEach(this::recordMetrics),
                persistOperationCounter,
                persistOperationMetadataStoreCounter,
                nonContiguousMessageRangeCounter,
                outgoingByteCounter,
                outgoingByteLogicalCounter,
                incomingByteCounter,
                ackOperationCounter,
                recoverOperationCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    public void incrementPersistUnackedRangesTruncated(ManagedCursor cursor) {
        persistUnackedRangesTruncated.add(1, cursor.getManagedCursorAttributes().getAttributes());
    }

    public void incrementPersistBatchDeletedIndexesTruncated(ManagedCursor cursor) {
        persistBatchDeletedIndexesTruncated.add(1, cursor.getManagedCursorAttributes().getAttributes());
    }

    private void recordMetrics(ManagedCursor cursor) {
        var stats = cursor.getStats();
        var cursorAttributesSet = cursor.getManagedCursorAttributes();
        var attributes = cursorAttributesSet.getAttributes();
        var attributesSucceed = cursorAttributesSet.getAttributesOperationSucceed();
        var attributesFailed = cursorAttributesSet.getAttributesOperationFailure();

        persistOperationCounter.record(stats.getPersistLedgerSucceed(), attributesSucceed);
        persistOperationCounter.record(stats.getPersistLedgerErrors(), attributesFailed);

        persistOperationMetadataStoreCounter.record(stats.getPersistZookeeperSucceed(), attributesSucceed);
        persistOperationMetadataStoreCounter.record(stats.getPersistZookeeperErrors(), attributesFailed);

        nonContiguousMessageRangeCounter.record(cursor.getTotalNonContiguousDeletedMessagesRange(), attributes);

        outgoingByteCounter.record(stats.getWriteCursorLedgerSize(), attributes);
        outgoingByteLogicalCounter.record(stats.getWriteCursorLedgerLogicalSize(), attributes);
        incomingByteCounter.record(stats.getReadCursorLedgerSize(), attributes);

        ackOperationCounter.record(stats.getAckCount(), attributes);
        recoverOperationCounter.record(stats.getRecoverSucceed(), attributesSucceed);
        recoverOperationCounter.record(stats.getRecoverErrors(), attributesFailed);
    }

    public void recordAckLatency(ManagedCursor cursor, double seconds) {
        var attributes = attributesOrNull(cursor);
        if (attributes != null) {
            ackLatency.record(seconds, attributes);
        }
    }

    public void recordPersistLatency(ManagedCursor cursor, double seconds) {
        var attributes = attributesOrNull(cursor);
        if (attributes != null) {
            persistLatency.record(seconds, attributes);
        }
    }

    public void recordRecoverLatency(ManagedCursor cursor, double seconds) {
        var attributes = attributesOrNull(cursor);
        if (attributes != null) {
            recoverLatency.record(seconds, attributes);
        }
    }

    private static Attributes attributesOrNull(ManagedCursor cursor) {
        try {
            return cursor.getManagedCursorAttributes().getAttributes();
        } catch (Exception e) {
            // Attribute resolution throws for non-topic-named managed ledgers (e.g. system
            // ledgers); recording telemetry must never break the caller's callback chain.
            return null;
        }
    }
}
