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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class ResourceGroupRateLimiterManagerTest {

    @Test
    public void testNewReplicationDispatchRateLimiterWithEmptyResourceGroup() {
        org.apache.pulsar.common.policies.data.ResourceGroup emptyResourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();

        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(emptyResourceGroup);
        assertFalse(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1L);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testReplicationDispatchRateLimiterOnMsgs() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs(10L);
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());


        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), resourceGroup.getReplicationDispatchRateInMsgs().longValue());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testReplicationDispatchRateLimiterOnBytes() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInBytes(20L);
        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup);
        assertTrue(resourceGroupDispatchLimiter.isDispatchRateLimitingEnabled());

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), -1L);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), resourceGroup.getReplicationDispatchRateInBytes().longValue());
    }

    @Test
    public void testUpdateReplicationDispatchRateLimiter() {
        org.apache.pulsar.common.policies.data.ResourceGroup resourceGroup =
                new org.apache.pulsar.common.policies.data.ResourceGroup();
        resourceGroup.setReplicationDispatchRateInMsgs(10L);
        resourceGroup.setReplicationDispatchRateInBytes(100L);

        ResourceGroupDispatchLimiter resourceGroupDispatchLimiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(resourceGroup);

        BytesAndMessagesCount quota = new BytesAndMessagesCount();
        quota.messages = 20;
        quota.bytes = 200;
        ResourceGroupRateLimiterManager.updateReplicationDispatchRateLimiter(resourceGroupDispatchLimiter, quota);

        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnByte(), quota.bytes);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnByte(), quota.bytes);
        assertEquals(resourceGroupDispatchLimiter.getAvailableDispatchRateLimitOnMsg(), quota.messages);
        assertEquals(resourceGroupDispatchLimiter.getDispatchRateOnMsg(), quota.messages);
    }

    @Test
    public void newReplicationDispatchRateLimiterWithValidData() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(100L, 1000L);
        assertEquals(limiter.getDispatchRateOnMsg(), 100L);
        assertEquals(limiter.getDispatchRateOnByte(), 1000L);
    }

    @Test
    public void newReplicationDispatchRateLimiterWithZeroValues() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(0L, 0L);
        assertEquals(limiter.getDispatchRateOnMsg(), -1L);
        assertEquals(limiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void newReplicationDispatchRateLimiterWithNegativeValues() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(-1L, -1L);
        assertEquals(limiter.getDispatchRateOnMsg(), -1L);
        assertEquals(limiter.getDispatchRateOnByte(), -1L);
    }

    @Test
    public void testConsumeDispatchQuotaWithAsyncTokenBucket() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(5L, 50L);

        limiter.consumeDispatchQuota(2, 20);

        assertEquals(limiter.getAvailableDispatchRateLimitOnMsg(), 3L);
        assertEquals(limiter.getAvailableDispatchRateLimitOnByte(), 30L);
    }

    @Test
    public void testLimiterRefillsAfterRatePeriod() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(2L, -1L);

        limiter.consumeDispatchQuota(2, 0);
        assertEquals(limiter.getAvailableDispatchRateLimitOnMsg(), 0L);

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(limiter.getAvailableDispatchRateLimitOnMsg(), 2L));
    }

    @Test
    public void testCloseDisablesLimiter() {
        ResourceGroupDispatchLimiter limiter =
                ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(5L, 50L);

        limiter.close();

        assertFalse(limiter.isDispatchRateLimitingEnabled());
        assertEquals(limiter.getDispatchRateOnMsg(), -1L);
        assertEquals(limiter.getDispatchRateOnByte(), -1L);
        assertEquals(limiter.getAvailableDispatchRateLimitOnMsg(), -1L);
        assertEquals(limiter.getAvailableDispatchRateLimitOnByte(), -1L);
    }
}
