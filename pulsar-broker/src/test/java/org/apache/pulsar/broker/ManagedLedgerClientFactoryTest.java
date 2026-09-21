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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;

/**
 * Unit test {@link ManagedLedgerClientFactory}.
 */
@Test(groups = "broker")
public class ManagedLedgerClientFactoryTest {

    /**
     * The per-msgLedger checkpoint maxEntrySize floor must fail at startup, before any
     * collaborator is used — so null collaborators are enough to drive the validation.
     */
    @Test
    public void testRejectsTooSmallCheckpointMaxEntrySizeAtStartup() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setPersistentUnackedRangesWithPerLedgerEntryEnabled(true);
        conf.setPersistentUnackedRangesMaxEntrySize(512);

        ManagedLedgerClientFactory factory = new ManagedLedgerClientFactory();
        assertThatThrownBy(() -> factory.initialize(conf, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("persistentUnackedRangesMaxEntrySize");
    }

    /**
     * With the feature disabled the value has no consumer, so the same 512 bytes must pass
     * validation. The null collaborators fail later (after the validation point) with some
     * other exception — asserting only that it is not the IllegalArgumentException from the
     * startup check keeps this robust to refactors.
     */
    @Test
    public void testSkipsCheckpointMaxEntrySizeValidationWhenFeatureDisabled() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setPersistentUnackedRangesWithPerLedgerEntryEnabled(false);
        conf.setPersistentUnackedRangesMaxEntrySize(512);

        ManagedLedgerClientFactory factory = new ManagedLedgerClientFactory();
        try {
            factory.initialize(conf, null, null, null, null);
        } catch (IllegalArgumentException e) {
            fail("maxEntrySize validation must not fire when the per-msgLedger checkpoint"
                    + " feature is disabled: " + e.getMessage());
        } catch (Exception expected) {
            // null collaborators fail after the validation point
        }
    }
}
