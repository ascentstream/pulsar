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
package org.apache.pulsar.broker.admin;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiReplicateSubscriptionsEnabledTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReplicateSubscriptionsEnabledOnNamespaceLevel() throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionsEnabled" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);
        assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));

        admin.namespaces().setReplicateSubscriptionsEnabled(nsName, true);
        assertTrue(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
            assertTrue(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
        });

        admin.namespaces().setReplicateSubscriptionsEnabled(nsName, false);
        assertFalse(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));
        await().untilAsserted(() -> {
            assertFalse(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
        });

        admin.namespaces().setReplicateSubscriptionsEnabled(nsName, null);
        assertNull(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
        });
    }

    @Test
    public void testReplicateSubscriptionsEnabledOnTopicLevel() throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionsEnabled" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);
        assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
        assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, true);
        await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true), Boolean.TRUE);
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false), Boolean.TRUE);
        });

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, false);
        await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true), Boolean.FALSE);
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false), Boolean.FALSE);
        });

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, null);
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
        });
    }

    @DataProvider
    Object[] replicateSubscriptionsEnabledPriorityLevelDataProvider() {
        return new Object[]{
                true,
                false,
                null,
        };
    }

    @Test(dataProvider = "replicateSubscriptionsEnabledPriorityLevelDataProvider")
    public void testReplicateSubscriptionsEnabledPriorityLevel(Boolean enabledOnNamespace)
            throws PulsarAdminException {
        String nsName = "public/testReplicateSubscriptionsEnabled" + System.nanoTime();
        admin.namespaces().createNamespace(nsName);
        assertNull(admin.namespaces().getReplicateSubscriptionsEnabled(nsName));
        admin.namespaces().setReplicateSubscriptionsEnabled(nsName, enabledOnNamespace);

        String topicName = nsName + "/topic" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topicName);

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, false);
        await().untilAsserted(() -> {
            assertFalse(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
            assertFalse(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
        });

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, true);
        await().untilAsserted(() -> {
            assertTrue(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true));
            assertTrue(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
        });

        admin.topicPolicies().setReplicateSubscriptionsEnabled(topicName, null);
        await().untilAsserted(() -> {
            assertNull(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, false));
            assertEquals(admin.topicPolicies().getReplicateSubscriptionsEnabled(topicName, true), enabledOnNamespace);
        });
    }
}
