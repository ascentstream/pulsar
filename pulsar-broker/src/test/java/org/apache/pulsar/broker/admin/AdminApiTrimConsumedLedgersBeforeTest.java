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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.Set;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test class for asyncTrimConsumedLedgersBefore admin API.
 *
 * This test verifies that the admin API methods exist and are callable.
 * Full functional testing with ManagedLedgerClientFactoryExt is in
 * the managed-ledger-ext module to avoid cyclic dependencies.
 *
 * Note: Due to module dependency constraints (managed-ledger-ext depends
 * on pulsar-broker), we cannot add managed-ledger-ext as a test dependency
 * in pulsar-broker without creating a cyclic dependency.
 */
@Test(groups = "broker-admin")
public class AdminApiTrimConsumedLedgersBeforeTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "trim-test";
    private final String testNamespace = "ns1";
    private final String myNamespace = testTenant + "/" + testNamespace;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Test that the admin API methods exist and have correct signatures.
     */
    @Test
    public void testTrimConsumedLedgersBeforeApiExists() {
        // This test verifies that the admin API methods are available
        // with the correct signatures

        // Test synchronous method exists
        try {
            admin.topics().getClass().getMethod("trimConsumedLedgersBefore", String.class, long.class);
        } catch (NoSuchMethodException e) {
            fail("trimConsumedLedgersBefore method should exist on Topics interface");
        }

        // Test asynchronous method exists
        try {
            admin.topics().getClass().getMethod("trimConsumedLedgersBeforeAsync", String.class, long.class);
        } catch (NoSuchMethodException e) {
            fail("trimConsumedLedgersBeforeAsync method should exist on Topics interface");
        }
    }

    /**
     * Test that the API methods are callable through admin client.
     * With default ManagedLedgerImpl, the operation will fail appropriately.
     */
    @Test
    public void testTrimConsumedLedgersBeforeApiCallable() throws Exception {
        String topicName = "persistent://" + myNamespace + "/test-api-callable";

        // Create a topic to ensure it exists
        try (Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create()) {
            producer.send("test-message".getBytes());
        }

        // Test that the API methods are callable
        // (with default implementation, they should fail appropriately)
        try {
            admin.topics().trimConsumedLedgersBefore(topicName, 12345L);
            fail("Should have thrown PulsarAdminException because "
                    + "asyncTrimConsumedLedgersBefore is not supported by default implementation");
        } catch (PulsarAdminException e) {
            // Expected - the default implementation doesn't support this operation
            assertNotNull(e.getMessage(), "Exception message should not be null");
        }
    }
}
