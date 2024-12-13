/**
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * Override "AbstractReplicator.producer" by {@param producer} and return the original value.
     */
    private ProducerImpl overrideProducerForReplicator(AbstractReplicator replicator, ProducerImpl newProducer)
            throws Exception {
        Field producerField = AbstractReplicator.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        ProducerImpl originalValue = (ProducerImpl) producerField.get(replicator);
        synchronized (replicator) {
            producerField.set(replicator, newProducer);
        }
        return originalValue;
    }

    @Test
    public void testReplicatorProducerStatInTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final String subscribeName = "subscribe_1";
        final byte[] msgValue = "test".getBytes();

        admin1.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscribeName, MessageId.earliest);
        admin2.topics().createSubscription(topicName, subscribeName, MessageId.earliest);

        // Verify replicator works.
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscribeName).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);

        // Verify there has one item in the attribute "publishers" or "replications"
        TopicStats topicStats2 = admin2.topics().getStats(topicName);
        Assert.assertTrue(topicStats2.getPublishers().size() + topicStats2.getReplication().size() > 0);

        // cleanup.
        consumer2.close();
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testCreateRemoteConsumerFirst() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();

        // The topic in cluster2 has a replicator created producer(schema Auto_Produce), but does not have any schema。
        // Verify: the consumer of this cluster2 can create successfully.
        Consumer<String> consumer2 = client2.newConsumer(Schema.STRING).topic(topicName).subscriptionName("s1")
                .subscribe();
        // Wait for replicator started.
        waitReplicatorStarted(topicName);
        // cleanup.
        producer1.close();
        consumer2.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        // Wait for replicator started.
        waitReplicatorStarted(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentReplicator replicator =
                (PersistentReplicator) persistentTopic.getReplicators().values().iterator().next();
        // Mock an error when calling "replicator.disconnect()"
        ProducerImpl mockProducer = Mockito.mock(ProducerImpl.class);
        Mockito.when(mockProducer.closeAsync()).thenReturn(FutureUtil.failedFuture(new Exception("mocked ex")));
        ProducerImpl originalProducer = overrideProducerForReplicator(replicator, mockProducer);
        // Verify: since the "replicator.producer.closeAsync()" will retry after it failed, the topic unload should be
        // successful.
        admin1.topics().unload(topicName);
        // Verify: After "replicator.producer.closeAsync()" retry again, the "replicator.producer" will be closed
        // successful.
        overrideProducerForReplicator(replicator, originalProducer);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(replicator.isConnected());
        });
        // cleanup.
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    private String getTheLatestMessage(String topic, PulsarClient client, PulsarAdmin admin) throws Exception {
        String dummySubscription = "s_" + UUID.randomUUID().toString().replace("-", "");
        admin.topics().createSubscription(topic, dummySubscription, MessageId.earliest);
        Consumer<String> c = client.newConsumer(Schema.STRING).topic(topic).subscriptionName(dummySubscription)
                .subscribe();
        String lastMsgValue = null;
        while (true) {
            Message<String> msg = c.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            lastMsgValue = msg.getValue();
        }
        c.unsubscribe();
        return lastMsgValue;
    }

    enum ReplicationLevel {
        TOPIC_LEVEL,
        NAMESPACE_LEVEL;
    }

    @DataProvider(name = "replicationLevels")
    public Object[][] replicationLevels() {
        return new Object[][]{
                {ReplicationLevel.TOPIC_LEVEL},
                {ReplicationLevel.NAMESPACE_LEVEL}
        };
    }

    @Test(dataProvider = "replicationLevels")
    public void testReloadWithTopicLevelGeoReplication(ReplicationLevel replicationLevel) throws Exception {
        final String topicName = ((Supplier<String>) () -> {
            if (replicationLevel.equals(ReplicationLevel.TOPIC_LEVEL)) {
                return BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
            } else {
                return BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
            }
        }).get();
        admin1.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        if (replicationLevel.equals(ReplicationLevel.TOPIC_LEVEL)) {
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        } else {
            pulsar1.getConfig().setTopicLevelPoliciesEnabled(false);
        }
        verifyReplicationWorks(topicName);

        /**
         * Verify:
         * 1. Inject an error to make the replicator is not able to work.
         * 2. Send one message, since the replicator does not work anymore, this message will not be replicated.
         * 3. Unload topic, the replicator will be re-created.
         * 4. Verify: the message can be replicated to the remote cluster.
         */
        // Step 1: Inject an error to make the replicator is not able to work.
        Replicator replicator = broker1.getTopic(topicName, false).join().get().getReplicators().get(cluster2);
        assertThat(replicator.terminate()).succeedsWithin(5, TimeUnit.SECONDS);

        // Step 2: Send one message, since the replicator does not work anymore, this message will not be replicated.
        String msg = UUID.randomUUID().toString();
        Producer p1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        p1.send(msg);
        p1.close();
        // The result of "peek message" will be the messages generated, so it is not the same as the message just sent.
        Thread.sleep(3000);
        assertNotEquals(getTheLatestMessage(topicName, client2, admin2), msg);
        assertEquals(admin1.topics().getStats(topicName).getReplication().get(cluster2).getReplicationBacklog(), 1);

        // Step 3: Unload topic, the replicator will be re-created.
        admin1.topics().unload(topicName);

        // Step 4. Verify: the message can be replicated to the remote cluster.
        Awaitility.await().atMost(Duration.ofSeconds(300)).untilAsserted(() -> {
            log.info("replication backlog: {}",
                    admin1.topics().getStats(topicName).getReplication().get(cluster2).getReplicationBacklog());
            assertEquals(admin1.topics().getStats(topicName).getReplication().get(cluster2).getReplicationBacklog(), 0);
            assertEquals(getTheLatestMessage(topicName, client2, admin2), msg);
        });

        // Cleanup.
        if (replicationLevel.equals(ReplicationLevel.TOPIC_LEVEL)) {
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(broker1.getTopic(topicName, false).join().get().getReplicators().size(), 0);
            });
            admin1.topics().delete(topicName, false);
            admin2.topics().delete(topicName, false);
        } else {
            pulsar1.getConfig().setTopicLevelPoliciesEnabled(true);
            cleanupTopics(() -> {
                admin1.topics().delete(topicName);
                admin2.topics().delete(topicName);
            });
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testCreateRemoteAdminFailed() throws Exception {
        final TenantInfo tenantInfo = admin1.tenants().getTenantInfo(defaultTenant);
        final String ns1 = defaultTenant + "/ns_" + UUID.randomUUID().toString().replace("-", "");
        final String randomClusterName = "c_" + UUID.randomUUID().toString().replace("-", "");
        final String topic = BrokerTestUtil.newUniqueName(ns1 + "/tp");
        admin1.namespaces().createNamespace(ns1);
        admin1.topics().createPartitionedTopic(topic, 2);

        // Inject a wrong cluster data which with empty fields.
        ClusterResources clusterResources = broker1.getPulsar().getPulsarResources().getClusterResources();
        clusterResources.createCluster(randomClusterName, ClusterData.builder().build());
        Set<String> allowedClusters = new HashSet<>(tenantInfo.getAllowedClusters());
        allowedClusters.add(randomClusterName);
        admin1.tenants().updateTenant(defaultTenant, TenantInfo.builder().adminRoles(tenantInfo.getAdminRoles())
                .allowedClusters(allowedClusters).build());

        // Verify.
        admin1.topics().setReplicationClusters(topic, Arrays.asList(cluster1, randomClusterName));

        // cleanup.
        admin1.topics().deletePartitionedTopic(topic);
        admin1.tenants().updateTenant(defaultTenant, tenantInfo);
    }

    protected void enableReplication(String topic) throws Exception {
        admin1.topics().setReplicationClusters(topic, Arrays.asList(cluster1, cluster2));
    }

    protected void disableReplication(String topic) throws Exception {
        admin1.topics().setReplicationClusters(topic, Arrays.asList(cluster1, cluster2));
    }

    @Test
    public void testConfigReplicationStartAt() throws Exception {
        // Initialize.
        String ns1 = defaultTenant + "/ns_" + UUID.randomUUID().toString().replace("-", "");
        String subscription1 = "s1";
        admin1.namespaces().createNamespace(ns1);
        if (!usingGlobalZK) {
            admin2.namespaces().createNamespace(ns1);
        }

        RetentionPolicies retentionPolicies = new RetentionPolicies(60 * 24, 1024);
        admin1.namespaces().setRetention(ns1, retentionPolicies);
        admin2.namespaces().setRetention(ns1, retentionPolicies);

        // 1. default config.
        // Enable replication for topic1.
        final String topic1 = BrokerTestUtil.newUniqueName("persistent://" + ns1 + "/tp_");
        admin1.topics().createNonPartitionedTopicAsync(topic1);
        admin1.topics().createSubscription(topic1, subscription1, MessageId.earliest);
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(topic1).create();
        p1.send("msg-1");
        p1.close();
        enableReplication(topic1);
        // Verify: since the replication was started at latest, there is no message to consume.
        Consumer<String> c1 = client2.newConsumer(Schema.STRING).topic(topic1).subscriptionName(subscription1)
                .subscribe();
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        assertNull(msg1);
        c1.close();
        disableReplication(topic1);

        // 2.Update config: start at "earliest".
        admin1.brokers().updateDynamicConfiguration("replicationStartAt", "earliest");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar1.getConfiguration().getReplicationStartAt(), "earliest");
        });

        final String topic2 = BrokerTestUtil.newUniqueName("persistent://" + ns1 + "/tp_");
        admin1.topics().createNonPartitionedTopicAsync(topic2);
        admin1.topics().createSubscription(topic2, subscription1, MessageId.earliest);
        Producer<String> p2 = client1.newProducer(Schema.STRING).topic(topic2).create();
        p2.send("msg-1");
        p2.close();
        enableReplication(topic2);
        // Verify: since the replication was started at earliest, there is one message to consume.
        Consumer<String> c2 = client2.newConsumer(Schema.STRING).topic(topic2).subscriptionName(subscription1)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Message<String> msg2 = c2.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg2);
        assertEquals(msg2.getValue(), "msg-1");
        c2.close();
        disableReplication(topic2);

        // 2.Update config: start at "latest".
        admin1.brokers().updateDynamicConfiguration("replicationStartAt", MessageId.latest.toString());
        Awaitility.await().untilAsserted(() -> {
            pulsar1.getConfiguration().getReplicationStartAt().equalsIgnoreCase("latest");
        });

        final String topic3 = BrokerTestUtil.newUniqueName("persistent://" + ns1 + "/tp_");
        admin1.topics().createNonPartitionedTopicAsync(topic3);
        admin1.topics().createSubscription(topic3, subscription1, MessageId.earliest);
        Producer<String> p3 = client1.newProducer(Schema.STRING).topic(topic3).create();
        p3.send("msg-1");
        p3.close();
        enableReplication(topic3);
        // Verify: since the replication was started at latest, there is no message to consume.
        Consumer<String> c3 = client2.newConsumer(Schema.STRING).topic(topic3).subscriptionName(subscription1)
                .subscribe();
        Message<String> msg3 = c3.receive(2, TimeUnit.SECONDS);
        assertNull(msg3);
        c3.close();
        disableReplication(topic3);

        // cleanup.
        // There is no good way to delete topics when using global ZK, skip cleanup.
        admin1.namespaces().setNamespaceReplicationClusters(ns1, Collections.singleton(cluster1));
        admin1.namespaces().unload(ns1);
        admin2.namespaces().setNamespaceReplicationClusters(ns1, Collections.singleton(cluster2));
        admin2.namespaces().unload(ns1);
        admin1.topics().delete(topic1, false);
        admin2.topics().delete(topic1, false);
        admin1.topics().delete(topic2, false);
        admin2.topics().delete(topic2, false);
        admin1.topics().delete(topic3, false);
        admin2.topics().delete(topic3, false);
    }

    @DataProvider(name = "replicationModes")
    public Object[][] replicationModes() {
        return new Object[][]{
                {ReplicationMode.OneWay},
                {ReplicationMode.DoubleWay}
        };
    }

    protected enum ReplicationMode {
        OneWay,
        DoubleWay;
    }

    @Test(dataProvider = "replicationModes")
    public void testDifferentTopicCreationRule(ReplicationMode replicationMode) throws Exception {
        String ns = defaultTenant + "/" + UUID.randomUUID().toString().replace("-", "");
        admin1.namespaces().createNamespace(ns);
        admin2.namespaces().createNamespace(ns);

        // Set topic auto-creation rule.
        // c1: no-partitioned topic
        // c2: partitioned topic with 2 partitions.
        AutoTopicCreationOverride autoTopicCreation =
                AutoTopicCreationOverrideImpl.builder().allowAutoTopicCreation(true)
                        .topicType("partitioned").defaultNumPartitions(2).build();
        admin2.namespaces().setAutoTopicCreation(ns, autoTopicCreation);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin2.namespaces().getAutoTopicCreationAsync(ns).join().getDefaultNumPartitions(),
                    Integer.valueOf(2));
            // Trigger system topic __change_event's initialize.
            pulsar2.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get("persistent://" + ns + "/1"));
        });

        // Create non-partitioned topic.
        // Enable replication.
        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        admin1.topics().createNonPartitionedTopic(tp);
        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        if (replicationMode.equals(ReplicationMode.DoubleWay)) {
            admin2.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        }

        // Trigger and wait for replicator starts.
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-1");
        p1.close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic = (PersistentTopic) broker1.getTopic(tp, false).join().get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        // Verify: the topics are the same between two clusters.
        Predicate<String> topicNameFilter = t -> {
            TopicName topicName = TopicName.get(t);
            if (!topicName.getNamespace().equals(ns)) {
                return false;
            }
            return t.startsWith(tp);
        };
        Awaitility.await().untilAsserted(() -> {
            List<String> topics1 = pulsar1.getBrokerService().getTopics().keys()
                    .stream().filter(topicNameFilter).collect(Collectors.toList());
            List<String> topics2 = pulsar2.getBrokerService().getTopics().keys()
                    .stream().filter(topicNameFilter).collect(Collectors.toList());
            Collections.sort(topics1);
            Collections.sort(topics2);
            assertEquals(topics1, topics2);
        });

        // cleanup.
        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1)));
        if (replicationMode.equals(ReplicationMode.DoubleWay)) {
            admin2.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster2)));
        }
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic = (PersistentTopic) broker1.getTopic(tp, false).join().get();
            assertTrue(persistentTopic.getReplicators().isEmpty());
            if (replicationMode.equals(ReplicationMode.DoubleWay)) {
                assertTrue(persistentTopic.getReplicators().isEmpty());
            }
        });
        admin1.topics().delete(tp, false);
        admin2.topics().delete(tp, false);
        admin1.namespaces().deleteNamespace(ns, true);
        admin2.namespaces().deleteNamespace(ns, true);
    }

    @DataProvider
    public Object[] isPartitioned() {
        return new Object[]{
                true,
                false
        };
    }

    @Test(dataProvider = "isPartitioned")
    public void testReplicatorCreateTopic(boolean isPartitioned) throws Exception {
        String ns = defaultTenant + "/" + UUID.randomUUID().toString().replace("-", "");
        admin1.namespaces().createNamespace(ns);
        admin2.namespaces().createNamespace(ns);

        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        if (isPartitioned) {
            admin1.topics().createPartitionedTopic(tp, 4);
        } else {
            admin1.topics().createNonPartitionedTopic(tp);
        }

        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker1.getTopic(
                                    isPartitioned ? TopicName.get(tp).getPartition(0).toString() : tp, false).join()
                            .get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        // Trigger the replicator.
        @Cleanup
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-1");

        Awaitility.await().untilAsserted(() -> {
            if (isPartitioned) {
                assertThat(admin2.topics().getPartitionedTopicList(ns)).contains(tp);
            } else {
                assertThat(admin2.topics().getList(ns)).contains(tp);
            }
        });
    }

    @Test
    public void testReplicatorCreateTopicWhenTopicExistsWithDifferentTypeAcrossClusters() throws Exception {
        String ns = defaultTenant + "/" + UUID.randomUUID().toString().replace("-", "");
        admin1.namespaces().createNamespace(ns);
        admin2.namespaces().createNamespace(ns);

        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        admin1.topics().createPartitionedTopic(tp, 4);
        admin2.topics().createNonPartitionedTopic(tp);

        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin2.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker1.getTopic(TopicName.get(tp).getPartition(0).toString(), false).join()
                            .get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic = (PersistentTopic) broker2.getTopic(tp, false).join().get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        // Trigger the replicator.
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-p1-1");
        p1.close();
        Producer<String> p2 = client2.newProducer(Schema.STRING).topic(tp).create();
        p2.send("msg-p2-1");
        p2.close();

        // The topic exists, but its type differs between the local and remote clusters. The replicator should not
        // recreate the topic.
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker1.getTopic(TopicName.get(tp).getPartition(0).toString(), false).join()
                            .get();
            persistentTopic.getReplicators().forEach((key, value) -> {
                assertFalse(value.isConnected());
            });
        });
        assertThat(admin2.topics().getPartitionedTopicList(ns)).doesNotContain(tp);

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic = (PersistentTopic) broker2.getTopic(tp, false).join().get();
            persistentTopic.getReplicators().forEach((key, value) -> {
                assertFalse(value.isConnected());
            });
        });
        assertThat(admin1.topics().getList(ns)).doesNotContain(tp);
    }

    @Test
    public void testReplicatorWhenPartitionCountsDiffer() throws Exception {
        String ns = defaultTenant + "/" + UUID.randomUUID().toString().replace("-", "");
        admin1.namespaces().createNamespace(ns);
        admin2.namespaces().createNamespace(ns);

        final String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp_");
        admin1.topics().createPartitionedTopic(tp, 4);
        admin2.topics().createPartitionedTopic(tp, 8);

        admin1.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin2.namespaces().setNamespaceReplicationClusters(ns, new HashSet<>(Arrays.asList(cluster1, cluster2)));

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker1.getTopic(TopicName.get(tp).getPartition(0).toString(), false).join()
                            .get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker2.getTopic(TopicName.get(tp).getPartition(0).toString(), false).join()
                            .get();
            assertFalse(persistentTopic.getReplicators().isEmpty());
        });

        // Trigger the replicator.
        @Cleanup
        Producer<String> p1 = client1.newProducer(Schema.STRING).topic(tp).create();
        p1.send("msg-p1-1");
        @Cleanup
        Producer<String> p2 = client2.newProducer(Schema.STRING).topic(tp).create();
        p2.send("msg-p2-1");

        // Topic partition counts differ between the local and remote clusters.
        // The replicator should not replicate the messages.
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) broker1.getTopic(TopicName.get(tp).getPartition(0).toString(), false).join()
                            .get();
            persistentTopic.getReplicators().forEach((key, value) -> {
                assertFalse(value.isConnected());
            });
        });

        @Cleanup
        Consumer<String> c2 = client2.newConsumer(Schema.STRING).topic(tp).subscriptionName("test-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        while (true) {
            Message<String> receive = c2.receive(3, TimeUnit.SECONDS);
            if (receive == null) {
                break;
            }
            assertEquals(receive.getValue(), "msg-p2-1");
        }
    }
}
