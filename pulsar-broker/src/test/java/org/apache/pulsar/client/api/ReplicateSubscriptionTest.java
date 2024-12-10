/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ReplicateSubscriptionTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerCacheEvictionIntervalMs(10000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setEnableReplicatedSubscriptions(true);
    }

    @DataProvider
    public Object[] replicateSubscriptionState() {
        return new Object[]{
                Boolean.TRUE,
                Boolean.FALSE,
                null
        };
    }

    @Test(dataProvider = "replicateSubscriptionState")
    public void testReplicateSubscriptionStateByConsumerBuilder(Boolean replicateSubscriptionState) {
        String topic = "persistent://my-property/my-ns/" + System.nanoTime();
        String subName = "sub-"+ System.nanoTime();
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName);
        if (replicateSubscriptionState != null) {
            consumerBuilder.replicateSubscriptionState(replicateSubscriptionState);
        }
        ConsumerBuilderImpl consumerBuilderImpl = (ConsumerBuilderImpl) consumerBuilder;
        assertEquals(consumerBuilderImpl.getConf().getReplicateSubscriptionState(), replicateSubscriptionState);
    }

    @Test(dataProvider = "replicateSubscriptionState")
    public void testReplicateSubscriptionStateNotNullByConsumerBuilder(Boolean replicateSubscriptionState) {
        String topic = "persistent://my-property/my-ns/" + System.nanoTime();
        String subName = "sub-"+ System.nanoTime();
        boolean enabled = replicateSubscriptionState != null && replicateSubscriptionState;
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .replicateSubscriptionState(enabled)
                .subscriptionName(subName);
        ConsumerBuilderImpl consumerBuilderImpl = (ConsumerBuilderImpl) consumerBuilder;
        assertEquals(consumerBuilderImpl.getConf().getReplicateSubscriptionState().booleanValue(), enabled);
    }

    @DataProvider
    public Object[][] replicateSubscriptionStateMultipleLevel() {
        return new Object[][]{
                // consumer level high priority.
                {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, true},
                {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, false},
                {Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, true},
                {Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, false},
                {Boolean.FALSE, Boolean.TRUE, Boolean.TRUE, true},
                {Boolean.FALSE, Boolean.TRUE, Boolean.TRUE, false},

                // namespace level high priority
                {null, Boolean.TRUE, null, true},
                {null, Boolean.TRUE, null, false},
                {null, Boolean.FALSE, null, true},
                {null, Boolean.FALSE, null, false},

                // topic level high priority.
                {null, Boolean.TRUE, Boolean.TRUE, true},
                {null, Boolean.TRUE, Boolean.TRUE, false},
                {null, Boolean.TRUE, Boolean.FALSE, true},
                {null, Boolean.TRUE, Boolean.FALSE, false},
                {null, Boolean.FALSE, Boolean.TRUE, true},
                {null, Boolean.FALSE, Boolean.TRUE, false},

                // All higher levels are null.
                {null, null, null, true},
                {null, null, null, false}
        };
    }

    /**
     * The priority list is from high to low: consumer/subscription, topic, namespace.
     */
    @Test(dataProvider = "replicateSubscriptionStateMultipleLevel")
    public void testReplicateSubscriptionStateByConsumerAndAdminAPI(
            Boolean consumerReplicateSubscriptionState,
            Boolean replicateSubscriptionEnabledOnNamespaceLevel,
            Boolean replicateSubscriptionEnabledOnTopicLevel,
            boolean replicatedSubscriptionStatus
    ) throws Exception {
        String nsName = "my-property/my-ns-" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        String topic = "persistent://" + nsName + "/" + System.nanoTime();
        String subName = "sub";
        @Cleanup
        Consumer<String> ignored = null;
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName);
        if (consumerReplicateSubscriptionState != null) {
            consumerBuilder.replicateSubscriptionState(consumerReplicateSubscriptionState);
        }
        ignored = consumerBuilder.subscribe();

        CompletableFuture<Optional<Topic>> topicIfExists = pulsar.getBrokerService().getTopicIfExists(topic);
        Optional<Topic> topicOptional = topicIfExists.get();
        assertTrue(topicOptional.isPresent());
        Topic topicRef = topicOptional.get();
        Subscription subscription = topicRef.getSubscription(subName);
        assertNotNull(subscription);
        PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;

        // Verify the consumer level.
        assertEquals(persistentSubscription.getReplicatedControlled(), consumerReplicateSubscriptionState);
        assertEquals(persistentSubscription.isReplicated(),
                consumerReplicateSubscriptionState != null && consumerReplicateSubscriptionState);

        // Verify the namespace level.
        admin.namespaces().setReplicateSubscriptionsEnabled(nsName, replicateSubscriptionEnabledOnNamespaceLevel);
        await().untilAsserted(() -> {
            assertEquals(admin.namespaces().getReplicateSubscriptionsEnabled(nsName),
                    replicateSubscriptionEnabledOnNamespaceLevel);
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topic, true),
                    replicateSubscriptionEnabledOnNamespaceLevel);
            if (consumerReplicateSubscriptionState == null) {
                // Using namespace policy.
                assertEquals(persistentSubscription.isReplicated(), replicateSubscriptionEnabledOnNamespaceLevel != null
                        && replicateSubscriptionEnabledOnNamespaceLevel);
            } else {
                // Using subscription policy.
                assertEquals(persistentSubscription.isReplicated(),
                        consumerReplicateSubscriptionState.booleanValue());
            }
        });

        // Verify the topic level.
        admin.topicPolicies().setReplicateSubscriptionsEnabled(topic, replicateSubscriptionEnabledOnTopicLevel);
        await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topic, false),
                    replicateSubscriptionEnabledOnTopicLevel);
            Boolean replicateSubscriptionsEnabled = admin.topicPolicies().getReplicateSubscriptionsEnabled(topic, true);
            assertTrue(replicateSubscriptionsEnabled == replicateSubscriptionEnabledOnTopicLevel
                    || replicateSubscriptionsEnabled == replicateSubscriptionEnabledOnNamespaceLevel);
            if (consumerReplicateSubscriptionState == null) {
                if (replicateSubscriptionEnabledOnTopicLevel != null) {
                    // Using topic policy.
                    assertEquals(persistentSubscription.isReplicated(),
                            replicateSubscriptionEnabledOnTopicLevel.booleanValue());
                } else {
                    // Using namespace policy.
                    assertEquals(persistentSubscription.isReplicated(),
                            replicateSubscriptionEnabledOnNamespaceLevel != null
                                    && replicateSubscriptionEnabledOnNamespaceLevel);
                }
            } else {
                // Using subscription policy.
                assertEquals(persistentSubscription.isReplicated(),
                        consumerReplicateSubscriptionState.booleanValue());
            }
        });

        // Verify the subscription level takes priority over the topic and namespace level.
        admin.topics().setReplicatedSubscriptionStatus(topic, subName, replicatedSubscriptionStatus);
        await().untilAsserted(() -> {
            assertEquals(persistentSubscription.isReplicated(), replicatedSubscriptionStatus);
        });
    }
}
