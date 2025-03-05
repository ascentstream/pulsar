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

package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import java.util.Map;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.testng.annotations.Test;

public class ReplicatorDispatchRateTest {

    @Test
    public void testGetReplicatorDispatchRateMap() {
        TopicPolicies topicPolicies = new TopicPolicies();
        String currentClusterName = "r1-cluster";

        // When the map is empty, which returns the default dispatch rate.
        Map<String, DispatchRateImpl> result = topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName);
        assertEquals(result.size(), 1);
        assertEquals(result.get(currentClusterName), topicPolicies.getReplicatorDispatchRate());

        // Put r1-cluster and dispatch rate to the map, and then check if the map includes that.
        String r1ClusterName = currentClusterName;
        DispatchRateImpl r1DispatchRate = DispatchRateImpl.builder().build();
        topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName).put(r1ClusterName, r1DispatchRate);
        result = topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName);
        assertEquals(result.size(), 1);
        assertEquals(result.get(r1ClusterName), r1DispatchRate);

        // Put r2-cluster and dispatch rate to the map, and then check if the map includes that.
        String r2ClusterName = "r2-cluster";
        DispatchRateImpl r2DispatchRate = DispatchRateImpl.builder().build();
        topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName).put(r2ClusterName, r2DispatchRate);
        result = topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName);
        assertEquals(result.size(), 2);
        assertEquals(result.get(r2ClusterName), r2DispatchRate);

        // Set up the default dispatch rate, the map is unchanged.
        DispatchRateImpl defaultDispatchRate = DispatchRateImpl.builder().build();
        topicPolicies.setReplicatorDispatchRate(defaultDispatchRate);
        result = topicPolicies.getFinalReplicatorDispatchRateMap(currentClusterName);
        assertEquals(result.size(), 2);
        assertEquals(result.get(r1ClusterName), r1DispatchRate);
        assertEquals(result.get(r2ClusterName), r2DispatchRate);
    }
}
