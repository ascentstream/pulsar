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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.PerChannelBookieClient;
import org.apache.bookkeeper.proto.PerChannelBookieClientPool;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class BrokerBkEnsemblesTests extends BkEnsemblesTestBase {

    public BrokerBkEnsemblesTests() {
        this(3);
    }

    public BrokerBkEnsemblesTests(int numberOfBookies) {
        super(numberOfBookies);
    }

    /**
     * It verifies that broker deletes cursor-ledger when broker-crashes without closing topic gracefully
     *
     * <pre>
     * 1. Create topic : publish/consume-ack msgs to update new cursor-ledger
     * 2. Verify cursor-ledger is created and ledger-znode present
     * 3. Broker crashes: remove topic and managed-ledgers without closing
     * 4. Recreate topic: publish/consume-ack msgs to update new cursor-ledger
     * 5. Topic is recovered from old-ledger and broker deletes the old ledger
     * 6. verify znode of old-ledger is deleted
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testCrashBrokerWithoutCursorLedgerLeak() throws Exception {

        ZooKeeper zk = bkEnsemble.getZkClient();
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/crash-broker";

        admin.namespaces().createNamespace(ns1);

        final String topic1 = "persistent://" + ns1 + "/my-topic";

        // (1) create topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .subscribe();
        Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        ManagedCursorImpl cursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor.getState().equals("Open"), 5, 100);

        // (2) validate cursor ledger is created and znode is present
        long cursorLedgerId = cursor.getCursorLedger();
        String ledgerPath = "/ledgers" + StringUtils.getHybridHierarchicalLedgerPath(cursorLedgerId);
        Assert.assertNotNull(zk.exists(ledgerPath, false));

        // (3) remove topic and managed-ledger from broker which means topic is not closed gracefully
        consumer.close();
        producer.close();
        pulsar.getBrokerService().removeTopicFromCache(topic);
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field
                .get(factory);
        ledgers.clear();

        // (4) Recreate topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        // (5) Broker should create new cursor-ledger and remove old cursor-ledger
        topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        final ManagedCursorImpl cursor1 = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor1.getState().equals("Open"), 5, 100);
        long newCursorLedgerId = cursor1.getCursorLedger();
        Assert.assertNotEquals(newCursorLedgerId, -1);
        Assert.assertNotEquals(cursorLedgerId, newCursorLedgerId);

        // cursor node must be deleted
        Awaitility.await().untilAsserted(() -> {
            Assert.assertNull(zk.exists(ledgerPath, false));
        });

        producer.close();
        consumer.close();
    }

    /**
     * It verifies broker-configuration using which broker can skip non-recoverable data-ledgers.
     *
     * <pre>
     * 1. publish messages in 5 data-ledgers each with 20 entries under managed-ledger
     * 2. delete first 4 data-ledgers
     * 3. consumer will fail to consume any message as first data-ledger is non-recoverable
     * 4. enable dynamic config to skip non-recoverable data-ledgers
     * 5. consumer will be able to consume 20 messages from last non-deleted ledger
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testSkipCorruptDataLedger() throws Exception {
        // Ensure intended state for autoSkipNonRecoverableData
        admin.brokers().updateDynamicConfiguration("autoSkipNonRecoverableData", "false");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/crash-broker";
        final int totalMessages = 99;
        final int totalDataLedgers = 5;
        final int entriesPerLedger = 20;

        try {
            admin.namespaces().createNamespace(ns1);
        } catch (Exception e) {

        }

        final String topic1 = "persistent://" + ns1 + "/my-topic-" + System.currentTimeMillis();

        // Create subscription
        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .receiverQueueSize(5).subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().iterator().next();
        // Create multiple data-ledger
        ManagedLedgerConfig config = ml.getConfig();
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        // bookkeeper client
        Field bookKeeperField = ManagedLedgerImpl.class.getDeclaredField("bookKeeper");
        bookKeeperField.setAccessible(true);
        // Create multiple data-ledger
        BookKeeper bookKeeper = (BookKeeper) bookKeeperField.get(ml);

        // (1) publish messages in 5 data-ledgers each with 20 entries under managed-ledger
        Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // validate: consumer is able to consume msg and close consumer after reading 1 entry
        Assert.assertNotNull(consumer.receive(1, TimeUnit.SECONDS));
        consumer.close();

        NavigableMap<Long, LedgerInfo> ledgerInfo = ml.getLedgersInfo();
        Assert.assertEquals(ledgerInfo.size(), totalDataLedgers);
        Entry<Long, LedgerInfo> lastLedger = ledgerInfo.lastEntry();

        // (2) delete first 4 data-ledgers
        ledgerInfo.entrySet().forEach(entry -> {
            if (!entry.equals(lastLedger)) {
                assertEquals(entry.getValue().getEntries(), entriesPerLedger);
                try {
                    bookKeeper.deleteLedger(entry.getKey());
                } catch (Exception e) {
                    log.warn("failed to delete ledger {}", entry.getKey(), e);
                }
            }
        });

        // clean managed-ledger and recreate topic to clean any data from the cache
        producer.close();
        pulsar.getBrokerService().removeTopicFromCache(topic);
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field
                .get(factory);
        ledgers.clear();

        // (3) consumer will fail to consume any message as first data-ledger is non-recoverable
        Message<byte[]> msg = null;
        // start consuming message
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        msg = consumer.receive(1, TimeUnit.SECONDS);
        Assert.assertNull(msg);
        consumer.close();

        // (4) enable dynamic config to skip non-recoverable data-ledgers
        admin.brokers().updateDynamicConfiguration("autoSkipNonRecoverableData", "true");

        retryStrategically((test) -> config.isAutoSkipNonRecoverableData(), 5, 100);

        // (5) consumer will be able to consume 19 messages from last non-deleted ledger
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        for (int i = 0; i < entriesPerLedger - 1; i++) {
            msg = consumer.receive();
            System.out.println(i);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
    }

    @Test
    public void testTruncateCorruptDataLedger() throws Exception {
        // Ensure intended state for autoSkipNonRecoverableData
        admin.brokers().updateDynamicConfiguration("autoSkipNonRecoverableData", "false");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final int totalMessages = 99;
        final int totalDataLedgers = 5;
        final int entriesPerLedger = 20;

        final String tenant = "prop";
        try {
            admin.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                    Sets.newHashSet(config.getClusterName())));
        } catch (Exception e) {

        }
        final String ns1 = tenant + "/crash-broker";
        try {
            admin.namespaces().createNamespace(ns1, Sets.newHashSet(config.getClusterName()));
        } catch (Exception e) {

        }

        final String topic1 = "persistent://" + ns1 + "/my-topic-" + System.currentTimeMillis();

        // Create subscription
        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .receiverQueueSize(5).subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().iterator().next();
        // Create multiple data-ledger
        ManagedLedgerConfig config = ml.getConfig();
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        // bookkeeper client
        Field bookKeeperField = ManagedLedgerImpl.class.getDeclaredField("bookKeeper");
        bookKeeperField.setAccessible(true);
        // Create multiple data-ledger
        BookKeeper bookKeeper = (BookKeeper) bookKeeperField.get(ml);

        // (1) publish messages in 10 data-ledgers each with 20 entries under managed-ledger
        Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // validate: consumer is able to consume msg and close consumer after reading 1 entry
        Assert.assertNotNull(consumer.receive(1, TimeUnit.SECONDS));
        consumer.close();

        NavigableMap<Long, LedgerInfo> ledgerInfo = ml.getLedgersInfo();
        Assert.assertEquals(ledgerInfo.size(), totalDataLedgers);
        Entry<Long, LedgerInfo> lastLedger = ledgerInfo.lastEntry();
        long firstLedgerToDelete = lastLedger.getKey();

        // (2) delete first 4 data-ledgers
        ledgerInfo.entrySet().forEach(entry -> {
            if (!entry.equals(lastLedger)) {
                assertEquals(entry.getValue().getEntries(), entriesPerLedger);
                try {
                    bookKeeper.deleteLedger(entry.getKey());
                } catch (Exception e) {
                    log.warn("failed to delete ledger {}", entry.getKey(), e);
                }
            }
        });

        // create 5 more ledgers
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message2-" + i;
            producer.send(message.getBytes());
        }

        // Admin should be able to truncate the topic
        admin.topics().truncate(topic1);

        ledgerInfo.entrySet().forEach(entry -> {
            log.warn("found ledger: {}", entry.getKey());
            assertNotEquals(firstLedgerToDelete, entry.getKey());
        });

        // Currently, ledger deletion is async and failed deletion
        // does not actually fail truncation but logs an exception
        // and creates scheduled task to retry
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            LedgerMetadata meta = bookKeeper
                    .getLedgerMetadata(firstLedgerToDelete)
                    .exceptionally(e -> null)
                    .get();
            assertEquals(null, meta, "ledger should be deleted " + firstLedgerToDelete);
            });

        // Should not throw, deleting absent ledger must be a noop
        // unless PulsarManager returned a wrong error which
        // got translated to BKUnexpectedConditionException
        try {
            bookKeeper.deleteLedger(firstLedgerToDelete);
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException bke) {
            // pass
        }

        producer.close();
        consumer.close();
    }

    @Test
    public void testDeleteLedgerFactoryCorruptLedger() throws Exception {
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("test");

        // bookkeeper client
        Field bookKeeperField = ManagedLedgerImpl.class.getDeclaredField("bookKeeper");
        bookKeeperField.setAccessible(true);
        // Create multiple data-ledger
        BookKeeper bookKeeper = (BookKeeper) bookKeeperField.get(ml);

        ml.addEntry("dummy-entry-1".getBytes());

        NavigableMap<Long, LedgerInfo> ledgerInfo = ml.getLedgersInfo();
        long lastLedger = ledgerInfo.lastEntry().getKey();

        ml.close();
        bookKeeper.deleteLedger(lastLedger);

        // BK ledger is deleted, factory should not throw on delete
        factory.delete("test");
    }

    @Test(timeOut = 20000)
    public void testTopicWithWildCardChar() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/topicWithSpecialChar";
        try {
            admin.namespaces().createNamespace(ns1);
        } catch (Exception e) {

        }

        final String topic1 = "persistent://"+ns1+"/`~!@#$%^&*()-_+=[]://{}|\\;:'\"<>,./?-30e04524";
        final String subName1 = "c1";
        final byte[] content = "test".getBytes();

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1).create();

        producer.send(content);
        Message<byte[]> msg = consumer.receive();
        Assert.assertEquals(msg.getData(), content);
        consumer.close();
        producer.close();
    }


    @Test
    public void testDeleteTopicWithMissingData() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("prop/usc");
        admin.namespaces().createNamespace(namespace);

        String topic = BrokerTestUtil.newUniqueName(namespace + "/my-topic");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test").subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.send("Hello");

        // Stop all bookies, to get all data offline
        bkEnsemble.stopBK();

        // Unload the topic. Since all the bookies are down, the recovery
        // will fail and the topic will not get reloaded.
        admin.topics().unload(topic);

        Thread.sleep(1000);

        CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopicIfExists(topic);
        try {
            future.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            // Expected
        }

        assertThrows(PulsarAdminException.ServerSideErrorException.class, () -> admin.topics().delete(topic));
    }

    @Test
    public void testDeleteTopicWithoutTopicLoaded() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("prop/usc");
        admin.namespaces().createNamespace(namespace);

        String topic = BrokerTestUtil.newUniqueName(namespace + "/my-topic");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.close();
        admin.topics().unload(topic);

        admin.topics().delete(topic);
        assertEquals(pulsar.getBrokerService().getTopicIfExists(topic).join(), Optional.empty());
    }

    /**
     * Injects a delay into the bookie-server response channel of the topic's current ledger, so that publishes
     * can be kept in flight while the test performs other actions on the topic.
     */
    protected Closeable injectBKServerDelayForCurrentLedger(String topic, long delayTime, TimeUnit unit,
                                                            int bkIndexOfEnsemble) throws Exception {
        // Make an injection to let the next publishing delay.
        ManagedLedgerFactoryImpl mlFactory = (ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory();
        BookieClientImpl bookieClient =
                (BookieClientImpl) mlFactory.getBookKeeper().get().getBookieClient();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        LedgerHandle ledgerHandle = ml.getCurrentLedger();
        BookieId bookieId1 = ledgerHandle.getLedgerMetadata().getEnsembleAt(bkIndexOfEnsemble).get(0);
        PerChannelBookieClientPool perChannelBookieClientPool = bookieClient.lookupClient(bookieId1);
        CompletableFuture<Channel> channelFuture = new CompletableFuture<>();
        perChannelBookieClientPool.obtain(new BookkeeperInternalCallbacks.GenericCallback<PerChannelBookieClient>() {
            @Override
            public void operationComplete(int rc, PerChannelBookieClient result) {
                channelFuture.complete(WhiteboxImpl.getInternalState(result, "channel"));
            }
        }, ledgerHandle.getId());
        Channel channel = channelFuture.get();

        ScheduledExecutorService bkServerIoMock =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bk-server-io-mock"));
        channel.eventLoop().execute(() -> {
            // To avoid client IO thread being stuck, we use a separate event loop to mock the server delay. And the
            // response handling is still in the client IO thread.
            channel.pipeline().addAfter("bookieProtoDecoder", "delayInjection", new ChannelInboundHandlerAdapter(){
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    bkServerIoMock.schedule(() -> {
                        // Call the real client io thread to handle the requests after a delayed time.
                        ctx.executor().execute(() -> {
                            ctx.fireChannelRead(msg);
                        });
                    }, delayTime, unit);
                }
            });
        });
        return () -> {
            CompletableFuture<Void> removing = new CompletableFuture<>();
            channel.eventLoop().execute(() -> {
                channel.pipeline().remove("delayInjection");
                removing.complete(null);
            });
            removing.whenComplete((__, ex) -> {
                bkServerIoMock.shutdown();
            });
        };
    }

    /**
     * Terminating a topic closes the current ledger under the publishes that are still in flight. The topic must not
     * stay fenced afterwards: a terminated topic still has to be drained, so consumers must be able to subscribe to it.
     */
    @Test
    public void testConsumerCanSubscribeAfterTerminateWithInFlightPublish() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("prop/ns");
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        admin.namespaces().createNamespace(namespace);
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Producer<String> producer = client.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic)
                .create();
        producer.send("1");
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topic, false).join().get();

        // Delay the bookie responses, so that the next publish is still in flight when the topic gets terminated.
        Closeable cancellation = injectBKServerDelayForCurrentLedger(topic, 10, TimeUnit.SECONDS, 0);
        producer.sendAsync("2");
        Awaitility.await().untilAsserted(() -> assertEquals(persistentTopic.getPendingWriteOps().get(), 1));

        CompletableFuture<MessageId> terminated = persistentTopic.terminate();
        // Under load, more publishes reach the terminated topic before its producers get disconnected.
        CompletableFuture<Exception> latePublish = new CompletableFuture<>();
        persistentTopic.publishMessage(Unpooled.wrappedBuffer("3".getBytes()),
                (e, ledgerId, entryId) -> latePublish.complete(e));
        terminated.get(10, TimeUnit.SECONDS);
        assertTrue(latePublish.get(10, TimeUnit.SECONDS) instanceof TopicTerminatedException);
        cancellation.close();

        // The publish that was in flight is failed, rather than being left pending forever.
        Awaitility.await().untilAsserted(() -> assertEquals(persistentTopic.getPendingWriteOps().get(), 0));

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "1");
    }
}
