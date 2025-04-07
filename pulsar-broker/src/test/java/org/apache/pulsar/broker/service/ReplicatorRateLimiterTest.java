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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupDispatchLimiter;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorRateLimiterTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    enum DispatchRateType {
        messageRate, byteRate
    }

    @DataProvider(name = "dispatchRateType")
    public Object[][] dispatchRateProvider() {
        return new Object[][] { { DispatchRateType.messageRate }, { DispatchRateType.byteRate } };
    }

    @Test
    public void testReplicatorRateLimiterWithOnlyTopicLevel() throws Exception {
        cleanup();
        config1.setDispatchThrottlingRatePerReplicatorInMsg(0); // disable broker level
        config1.setDispatchThrottlingRatePerReplicatorInByte(0L);
        setup();

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/testReplicatorRateLimiterWithOnlyTopicLevel";

        admin1.namespaces().createNamespace(namespace);
        // set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
            .statsInterval(0, TimeUnit.SECONDS).build();
        client1.newProducer().topic(topicName).create().close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        // rate limiter disable by default
        assertFalse(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertFalse(topic.getReplicators().values().get(0).getResourceGroupDispatchRateLimiter().isPresent());

        //set topic-level policy, which should take effect
        DispatchRate topicRate = DispatchRate.builder()
            .dispatchThrottlingRateInMsg(10)
            .dispatchThrottlingRateInByte(20)
            .ratePeriodInSecond(30)
            .build();
        admin1.topics().setReplicatorDispatchRate(topicName, topicRate);
        Awaitility.await().untilAsserted(() ->
            assertEquals(admin1.topics().getReplicatorDispatchRate(topicName), topicRate));
        assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 10);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //remove topic-level policy
        admin1.topics().removeReplicatorDispatchRate(topicName);
        Awaitility.await().untilAsserted(() ->
            assertNull(admin1.topics().getReplicatorDispatchRate(topicName)));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), -1);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
            -1L);

        // ResourceGroupDispatchRateLimiter
        String resourceGroupName = UUID.randomUUID().toString();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setReplicationDispatchRateInBytes(10L);
        resourceGroup.setReplicationDispatchRateInMsgs(20L);
        admin1.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin1.resourcegroups()
                .getResourceGroup(resourceGroupName)));
        admin1.topicPolicies().setResourceGroup(topicName, resourceGroupName);

        Replicator replicator = topic.getReplicators().values().get(0);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(replicator.getResourceGroupDispatchRateLimiter().isPresent());
            ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = replicator.getResourceGroupDispatchRateLimiter().get();
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());
        });
    }

    @Test
    public void testReplicatorRateLimiterWithOnlyNamespaceLevel() throws Exception {
        cleanup();
        config1.setDispatchThrottlingRatePerReplicatorInMsg(0); // disable broker level
        config1.setDispatchThrottlingRatePerReplicatorInByte(0L);
        setup();

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/testReplicatorRateLimiterWithOnlyNamespaceLevel";

        admin1.namespaces().createNamespace(namespace);
        // set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
            .statsInterval(0, TimeUnit.SECONDS).build();
        client1.newProducer().topic(topicName).create().close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        // rate limiter disable by default
        assertFalse(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertFalse(topic.getReplicators().values().get(0).getResourceGroupDispatchRateLimiter().isPresent());

        //set namespace-level policy, which should take effect
        DispatchRate topicRate = DispatchRate.builder()
            .dispatchThrottlingRateInMsg(10)
            .dispatchThrottlingRateInByte(20)
            .ratePeriodInSecond(30)
            .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, topicRate);
        Awaitility.await().untilAsserted(() ->
            assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), topicRate));
        assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 10);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //remove topic-level policy
        admin1.namespaces().removeReplicatorDispatchRate(namespace);
        Awaitility.await().untilAsserted(() ->
            assertNull(admin1.namespaces().getReplicatorDispatchRate(namespace)));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), -1);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
            -1L);

        // ResourceGroupDispatchRateLimiter
        String resourceGroupName = UUID.randomUUID().toString();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setReplicationDispatchRateInBytes(10L);
        resourceGroup.setReplicationDispatchRateInMsgs(20L);
        admin1.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin1.resourcegroups()
                .getResourceGroup(resourceGroupName)));
        admin1.namespaces().setNamespaceResourceGroup(namespace, resourceGroupName);

        Replicator replicator = topic.getReplicators().values().get(0);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(replicator.getResourceGroupDispatchRateLimiter().isPresent());
            ResourceGroupDispatchLimiter resourceGroupDispatchLimiter = replicator.getResourceGroupDispatchRateLimiter().get();
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
            assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());
        });
    }

    @Test
    public void testReplicatorRateLimiterWithOnlyBrokerLevel() throws Exception {
        cleanup();
        config1.setDispatchThrottlingRatePerReplicatorInMsg(0); // disable broker level when init
        config1.setDispatchThrottlingRatePerReplicatorInByte(0L);
        setup();

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/testReplicatorRateLimiterWithOnlyBrokerLevel";

        admin1.namespaces().createNamespace(namespace);
        // set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
            .statsInterval(0, TimeUnit.SECONDS).build();
        client1.newProducer().topic(topicName).create().close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        // rate limiter disable by default
        assertFalse(topic.getReplicators().values().get(0).getRateLimiter().isPresent());

        //set broker-level policy, which should take effect
        admin1.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerReplicatorInMsg", "10");
        admin1.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerReplicatorInByte", "20");
        Awaitility.await().untilAsserted(() -> {
            assertTrue(admin1.brokers()
                .getAllDynamicConfigurations().containsKey("dispatchThrottlingRatePerReplicatorInByte"));
            assertEquals(admin1.brokers()
                .getAllDynamicConfigurations().get("dispatchThrottlingRatePerReplicatorInMsg"), "10");
            assertEquals(admin1.brokers()
                .getAllDynamicConfigurations().get("dispatchThrottlingRatePerReplicatorInByte"), "20");
        });

        assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 10);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);
    }

    @Test
    public void testReplicatorRatePriority() throws Exception {
        cleanup();
        config1.setDispatchThrottlingRatePerReplicatorInMsg(100);
        config1.setDispatchThrottlingRatePerReplicatorInByte(200L);
        setup();

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/ratechange";

        admin1.namespaces().createNamespace(namespace);
        // set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        client1.newProducer().topic(topicName).create().close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        //use broker-level by default
        assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 100);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 200L);

        //set namespace-level policy, which should take effect
        DispatchRate nsDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(50)
                .dispatchThrottlingRateInByte(60)
                .ratePeriodInSecond(60)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, nsDispatchRate);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), nsDispatchRate));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 50);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 60L);

        //set topic-level policy, which should take effect
        DispatchRate topicRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(20)
                .ratePeriodInSecond(30)
                .build();
        admin1.topics().setReplicatorDispatchRate(topicName, topicRate);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin1.topics().getReplicatorDispatchRate(topicName), topicRate));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 10);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //Set the namespace-level policy, which should not take effect
        DispatchRate nsDispatchRate2 = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(500)
                .dispatchThrottlingRateInByte(600)
                .ratePeriodInSecond(700)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, nsDispatchRate2);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), nsDispatchRate2));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //remove topic-level policy, namespace-level should take effect
        admin1.topics().removeReplicatorDispatchRate(topicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin1.topics().getReplicatorDispatchRate(topicName)));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 500);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
                600L);

        //remove namespace-level policy, broker-level should take effect
        admin1.namespaces().setReplicatorDispatchRate(namespace, null);
        Awaitility.await().untilAsserted(() ->
                assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 100));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
                200L);
    }

    /**
     * verifies dispatch rate for replicators get changed once namespace policies changed.
     *
     * 1. verify default replicator not configured.
     * 2. change namespace setting of replicator dispatchRateMsg, verify topic changed.
     * 3. change namespace setting of replicator dispatchRateByte, verify topic changed.
     *
     * @throws Exception
     */
    @Test
    public void testReplicatorRateLimiterDynamicallyChange() throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/ratechange";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        // 1. default replicator throttling not configured
        Assert.assertFalse(topic.getReplicators().values().get(0).getRateLimiter().isPresent());

        // 2. change namespace setting of replicator dispatchRateMsg, verify topic changed.
        int messageRate = 100;
        DispatchRate dispatchRateMsg = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRateMsg);

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);

        // 3. change namespace setting of replicator dispatchRateByte, verify topic changed.
        messageRate = 500;
        DispatchRate dispatchRateByte = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(messageRate)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRateByte);
        replicatorUpdated = false;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte() == messageRate) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), dispatchRateByte);
    }

    /**
     * verifies dispatch rate for replicators works well for both Message limit and Byte limit .
     *
     * 1. verify topic replicator get configured.
     * 2. namespace setting of replicator dispatchRate, verify consumer in other cluster could not receive all messages.
     *
     * @throws Exception
     */
    @Test(dataProvider =  "dispatchRateType")
    public void testReplicatorRateLimiterMessageNotReceivedAllMessages(DispatchRateType dispatchRateType) throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatorbyteandmsg-" + dispatchRateType.toString() + "-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/notReceivedAll";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int messageRate = 100;
        DispatchRate dispatchRate;
        if (DispatchRateType.messageRate.equals(dispatchRateType)) {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(messageRate)
                    .dispatchThrottlingRateInByte(-1)
                    .ratePeriodInSecond(360)
                    .build();
        } else {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(-1)
                    .dispatchThrottlingRateInByte(messageRate)
                    .ratePeriodInSecond(360)
                    .build();
        }
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        if (DispatchRateType.messageRate.equals(dispatchRateType)) {
            Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);
        } else {
            Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), messageRate);
        }

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2").messageListener((c1, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            totalReceived.incrementAndGet();
        }).subscribe();

        int numMessages = 500;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertTrue(totalReceived.get() < messageRate * 2);

        consumer.close();
        producer.close();
    }

    /**
     * verifies dispatch rate for replicators works well for both Message limit.
     *
     * 1. verify topic replicator get configured.
     * 2. namespace setting of replicator dispatchRate,
     *      verify consumer in other cluster could receive all messages < message limit.
     * 3. verify consumer in other cluster could not receive all messages > message limit.
     *
     * @throws Exception
     */
    @Test
    public void testReplicatorRateLimiterMessageReceivedAllMessages() throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatormsg-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/notReceivedAll";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int messageRate = 100;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2").messageListener((c1, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            totalReceived.incrementAndGet();
        }).subscribe();

        int numMessages = 50;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        Thread.sleep(1000);
        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertEquals(totalReceived.get(), numMessages);


        numMessages = 200;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }
        Thread.sleep(1000);
        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertEquals(totalReceived.get(), messageRate);

        consumer.close();
        producer.close();
    }

    @Test
    public void testResourceGroupReplicatorRateLimiter() throws Exception {
        final String namespace = "pulsar/replicatormsg-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/" + UUID.randomUUID();

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        // ResourceGroupDispatchRateLimiter
        int messageRate = 100;
        String resourceGroupName = UUID.randomUUID().toString();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs((long) messageRate);
        admin1.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin1.resourcegroups()
                .getResourceGroup(resourceGroupName)));
        admin1.namespaces().setNamespaceResourceGroup(namespace, resourceGroupName);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Producer<byte[]> producer = client1.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        @Cleanup
        Consumer<byte[]> consumer = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2").messageListener((c1, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            totalReceived.incrementAndGet();
        }).subscribe();

        int numMessages = 500;
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        Assert.assertTrue(totalReceived.get() < messageRate * 2);
    }

    @Test
    public void testLoadResourceGroupReplicatorRateLimiter() throws Exception {
        final String namespace = "pulsar/replicatormsg-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/" + UUID.randomUUID();

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        // ResourceGroupDispatchRateLimiter
        long messageRate = 100;
        long byteRate = 500000;
        String resourceGroupName = UUID.randomUUID().toString();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs(messageRate);
        resourceGroup.setReplicationDispatchRateInBytes(byteRate);
        admin1.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin1.resourcegroups()
                .getResourceGroup(resourceGroupName)));
        admin1.namespaces().setNamespaceResourceGroup(namespace, resourceGroupName);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Producer<byte[]> producer = client1.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send(new byte[1]);

        CompletableFuture<Optional<Topic>> topicIfExists = pulsar1.getBrokerService().getTopicIfExists(topicName);
        assertThat(topicIfExists).succeedsWithin(3, TimeUnit.SECONDS);
        Optional<Topic> topicOptional = topicIfExists.get();
        assertTrue(topicOptional.isPresent());
        PersistentTopic persistentTopic = (PersistentTopic) topicOptional.get();

        Replicator r2Replicator = persistentTopic.getReplicators().get("r2");
        assertNotNull(r2Replicator);
        Optional<ResourceGroupDispatchLimiter> resourceGroupDispatchRateLimiter =
                r2Replicator.getResourceGroupDispatchRateLimiter();
        assertTrue(resourceGroupDispatchRateLimiter.isPresent());
        assertEquals(resourceGroupDispatchRateLimiter.get().getDispatchRateOnMsg(), messageRate);
        assertEquals(resourceGroupDispatchRateLimiter.get().getDispatchRateOnByte(), byteRate);

        // Set up rate limiter for r1 -> r2 channel.
        long messageRateBetweenR1AndR2 = 1000;
        long byteRateBetweenR1AndR2 = 5000000;
        admin1.resourcegroups().setReplicatorDispatchRate(resourceGroupName, "r2",
                DispatchRate.builder()
                        .dispatchThrottlingRateInByte(byteRateBetweenR1AndR2)
                        .dispatchThrottlingRateInMsg((int) messageRateBetweenR1AndR2)
                        .build());
        Awaitility.await().untilAsserted(() -> {
            Optional<ResourceGroupDispatchLimiter> rateLimiter = r2Replicator.getResourceGroupDispatchRateLimiter();
            assertTrue(rateLimiter.isPresent());
            assertEquals(rateLimiter.get().getDispatchRateOnMsg(), messageRateBetweenR1AndR2);
            assertEquals(rateLimiter.get().getDispatchRateOnByte(), byteRateBetweenR1AndR2);
        });

        // Remove rate limiter for r1 -> r2 channel, and then use the default rate limiter.
        admin1.resourcegroups().removeReplicatorDispatchRate(resourceGroupName, "r2");
        Awaitility.await().untilAsserted(() -> {
            Optional<ResourceGroupDispatchLimiter> rateLimiter = r2Replicator.getResourceGroupDispatchRateLimiter();
            assertTrue(rateLimiter.isPresent());
            assertEquals(rateLimiter.get().getDispatchRateOnMsg(), messageRate);
            assertEquals(rateLimiter.get().getDispatchRateOnByte(), byteRate);
        });
    }

    @Test
    public void testReplicatorRateLimiterByBytes() throws Exception {
        final String namespace = "pulsar/replicatormsg-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/RateLimiterByBytes";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int byteRate = 400;
        final int payloadSize = 100;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(byteRate)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).build();
        @Cleanup
        Producer<byte[]> producer = client1.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        Awaitility.await()
                .untilAsserted(() -> assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent()));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), byteRate);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString())
                .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        @Cleanup
        Consumer<byte[]> ignored = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2")
                .messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();

        // The total bytes is 5 times the rate limit value.
        int numMessages = byteRate / payloadSize * 5;
        for (int i = 0; i < numMessages * payloadSize; i++) {
            producer.send(new byte[payloadSize]);
        }

        Awaitility.await().pollDelay(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    // The rate limit occurs in the next reading cycle, so a value fault tolerance needs to be added.
                    assertThat(totalReceived.get()).isLessThan((byteRate / payloadSize) + 2);
                });
    }

    @Test
    public void testLoadReplicatorDispatchRateLimiterByTopicPolicies() throws Exception {
        final String namespace = "pulsar/replicator-dispatch-rate-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/" + System.currentTimeMillis();

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int byteRate = 400;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(byteRate)
                .ratePeriodInSecond(1)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        admin1.topics().createNonPartitionedTopic(topicName);

        Optional<Topic> topicOptional = pulsar1.getBrokerService().getTopicIfExists(topicName).get();
        assertTrue(topicOptional.isPresent());

        PersistentTopic topic = (PersistentTopic) topicOptional.get();
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(), byteRate);
                });

        // r1 -> r2: limits this replication channel by default rate limit on the namespace level.
        long dispatchThrottlingRateInMsgOnNs = 1000;
        long dispatchThrottlingRateInByteOnNs = 3000;
        DispatchRate dispatchRateOnNamespace = DispatchRate.builder()
                .dispatchThrottlingRateInMsg((int) dispatchThrottlingRateInMsgOnNs)
                .dispatchThrottlingRateInByte(dispatchThrottlingRateInByteOnNs)
                .ratePeriodInSecond(1)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRateOnNamespace);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(), dispatchThrottlingRateInByteOnNs);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(), dispatchThrottlingRateInMsgOnNs);
                });

        // r1 -> r2: limits this replication channel by the specific cluster on the namespace level.
        long dispatchThrottlingRateInMsgWhenR1ToR2OnNs = 3000;
        long dispatchThrottlingRateInByteWhenR1ToR2OnNs = 2000;
        DispatchRate dispatchThrottlingRateInMsgBetweenR1AndR2OnNs = DispatchRate.builder()
                .dispatchThrottlingRateInMsg((int) dispatchThrottlingRateInMsgWhenR1ToR2OnNs)
                .dispatchThrottlingRateInByte(dispatchThrottlingRateInByteWhenR1ToR2OnNs)
                .ratePeriodInSecond(1)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, "r2", dispatchThrottlingRateInMsgBetweenR1AndR2OnNs);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(), dispatchThrottlingRateInByteWhenR1ToR2OnNs);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(), dispatchThrottlingRateInMsgWhenR1ToR2OnNs);
                });

        // r1 -> r2: limits this replication channel by default dispatch rate on the topic level.
        long defaultDispatchThrottlingRateInMsgOnTopic = 30000;
        long defaultDispatchThrottlingRateInByteOnTopic = 20000;
        DispatchRate dispatchRateOnTopic = DispatchRate.builder()
                .dispatchThrottlingRateInMsg((int) defaultDispatchThrottlingRateInMsgOnTopic)
                .dispatchThrottlingRateInByte(defaultDispatchThrottlingRateInByteOnTopic)
                .ratePeriodInSecond(1)
                .build();

        admin1.topicPolicies().setReplicatorDispatchRate(topicName, dispatchRateOnTopic);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(),
                            defaultDispatchThrottlingRateInByteOnTopic);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(),
                            defaultDispatchThrottlingRateInMsgOnTopic);
                });

        // r1 -> r2: limits this replication channel by the specific cluster on the topic level.
        long dispatchThrottlingRateInMsgBetweenR1AndR2OnTopic = 50000;
        long dispatchThrottlingRateInByteBetweenR1AndR2OnTopic = 40000;
        DispatchRate dispatchRateBetweenR1AndR2OnTopic = DispatchRate.builder()
                .dispatchThrottlingRateInMsg((int) dispatchThrottlingRateInMsgBetweenR1AndR2OnTopic)
                .dispatchThrottlingRateInByte(dispatchThrottlingRateInByteBetweenR1AndR2OnTopic)
                .ratePeriodInSecond(1)
                .build();

        admin1.topicPolicies().setReplicatorDispatchRate(topicName, "r2", dispatchRateBetweenR1AndR2OnTopic);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(),
                            dispatchThrottlingRateInByteBetweenR1AndR2OnTopic);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(),
                            dispatchThrottlingRateInMsgBetweenR1AndR2OnTopic);
                });

        // r1 -> r2: removes the specific cluster rate limit from the topic level, and then will use the default rate
        // limit from the topic level.
        admin1.topicPolicies().removeReplicatorDispatchRate(topicName, "r2");
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(),
                            defaultDispatchThrottlingRateInByteOnTopic);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(),
                            defaultDispatchThrottlingRateInMsgOnTopic);
                });

        // r1 -> r2: removes the default rate limit from the topic level, and then will use the specific cluster rate
        // limit from the namespace level.
        admin1.topicPolicies().removeReplicatorDispatchRate(topicName);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(),
                            dispatchThrottlingRateInByteWhenR1ToR2OnNs);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(),
                            dispatchThrottlingRateInMsgWhenR1ToR2OnNs);
                });

        // r1 -> r2: removes the specific cluster rate limit from the namespace level, and then will use the default
        // rate limit from the namespace level.
        admin1.namespaces().removeReplicatorDispatchRate(namespace, "r2");
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(),
                            dispatchThrottlingRateInByteOnNs);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(),
                            dispatchThrottlingRateInMsgOnNs);
                });

        // r1 -> r2: removes the default cluster rate limit from the namespace level, and then will use the
        // rate limit from the broker level.
        admin1.namespaces().removeReplicatorDispatchRate(namespace);
        Awaitility.await()
                .untilAsserted(() -> {
                    Optional<DispatchRateLimiter> rateLimiter = topic.getReplicators().values().get(0).getRateLimiter();
                    assertTrue(rateLimiter.isPresent());
                    assertEquals(rateLimiter.get().getDispatchRateOnByte(), -1);
                    assertEquals(rateLimiter.get().getDispatchRateOnMsg(), -1);
                });
    }


    private static final Logger log = LoggerFactory.getLogger(ReplicatorRateLimiterTest.class);
}
