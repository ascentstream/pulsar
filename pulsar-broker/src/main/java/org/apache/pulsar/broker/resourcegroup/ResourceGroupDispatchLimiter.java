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

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;

public class ResourceGroupDispatchLimiter implements AutoCloseable {
    private static final long RATE_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(1);

    private volatile AsyncTokenBucket dispatchRateLimiterOnMessage;
    private volatile AsyncTokenBucket dispatchRateLimiterOnByte;

    public ResourceGroupDispatchLimiter(long dispatchRateInMsgs, long dispatchRateInBytes) {
        update(dispatchRateInMsgs, dispatchRateInBytes);
    }

    public void update(long dispatchRateInMsgs, long dispatchRateInBytes) {
        dispatchRateLimiterOnMessage = createOrReuseTokenBucket(dispatchRateLimiterOnMessage, dispatchRateInMsgs);
        dispatchRateLimiterOnByte = createOrReuseTokenBucket(dispatchRateLimiterOnByte, dispatchRateInBytes);
    }

    private AsyncTokenBucket createOrReuseTokenBucket(AsyncTokenBucket currentLimiter, long rate) {
        if (rate <= 0) {
            return null;
        }
        if (currentLimiter != null && currentLimiter.getRate() == rate) {
            return currentLimiter;
        }
        return AsyncTokenBucket.builder()
                .rate(rate)
                .ratePeriodNanos(RATE_PERIOD_NANOS)
                .addTokensResolutionNanos(RATE_PERIOD_NANOS)
                .build();
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage == null ? -1
                : Math.max(localDispatchRateLimiterOnMessage.getTokens(), 0);
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnByte() {
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte == null ? -1
                : Math.max(localDispatchRateLimiterOnByte.getTokens(), 0);
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    public void consumeDispatchQuota(long numberOfMessages, long byteSize) {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        if (numberOfMessages > 0 && localDispatchRateLimiterOnMessage != null) {
            localDispatchRateLimiterOnMessage.consumeTokens(numberOfMessages);
        }
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        if (byteSize > 0 && localDispatchRateLimiterOnByte != null) {
            localDispatchRateLimiterOnByte.consumeTokens(byteSize);
        }
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    public boolean tryAcquire(long numberOfMessages, long byteSize) {
        boolean res = true;
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        if (numberOfMessages > 0 && localDispatchRateLimiterOnMessage != null) {
            res &= !localDispatchRateLimiterOnMessage.consumeTokensAndCheckIfContainsTokens(numberOfMessages);
        }
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        if (byteSize > 0 && localDispatchRateLimiterOnByte != null) {
            res &= !localDispatchRateLimiterOnByte.consumeTokensAndCheckIfContainsTokens(byteSize);
        }

        return res;
    }

    /**
     * Checks if dispatch-rate limiting is enabled.
     *
     * @return
     */
    public boolean isDispatchRateLimitingEnabled() {
        return dispatchRateLimiterOnMessage != null || dispatchRateLimiterOnByte != null;
    }

    public void close() {
        dispatchRateLimiterOnMessage = null;
        dispatchRateLimiterOnByte = null;
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnMsg() {
        AsyncTokenBucket localDispatchRateLimiterOnMessage = dispatchRateLimiterOnMessage;
        return localDispatchRateLimiterOnMessage != null ? localDispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnByte() {
        AsyncTokenBucket localDispatchRateLimiterOnByte = dispatchRateLimiterOnByte;
        return localDispatchRateLimiterOnByte != null ? localDispatchRateLimiterOnByte.getRate() : -1;
    }


}
