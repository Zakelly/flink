/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fixed capacity cache limit policy, which is not thread-safe, and the thread safe should be
 * ensured by the invoker.
 */
public class SizeBasedFloatCountCacheLimitPolicy implements CacheLimitPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(SizeBasedFloatCountCacheLimitPolicy.class);

    /** The capacity. */
    private final long capacity;

    /** The usage size. */
    private long usageSize;

    private final AtomicInteger countLimit;

    private int count;

    public SizeBasedFloatCountCacheLimitPolicy(long capacity, AtomicInteger countLimit) {
        this.capacity = capacity;
        this.usageSize = 0;
        this.countLimit = countLimit;
        this.count = 0;
        LOG.info("Creating SizeBasedCacheLimitPolicy with capacity {}", capacity);
    }

    @Override
    public boolean isSafeToAdd(long toAddSize) {
        return toAddSize < capacity && countLimit.get() > 0;
    }

    @Override
    public boolean isOverflow(long toAddSize) {
        return usageSize + toAddSize > capacity || count >= countLimit.get();
    }

    @Override
    public void acquire(long toAddSize) {
        usageSize += toAddSize;
        count += 1;
    }

    @Override
    public void release(long toReleaseSize) {
        usageSize -= Math.min(usageSize, toReleaseSize);
        count -= 1;
    }

    @Override
    public long usedBytes() {
        return usageSize;
    }

    @Override
    public void registerCustomizedMetrics(String prefix, MetricGroup metricGroup) {}

    @Override
    public String toString() {
        return "SizeBasedFloatCountCacheLimitPolicy{"
                + "capacity="
                + capacity
                + ", usageSize="
                + usageSize
                + '}';
    }
}
