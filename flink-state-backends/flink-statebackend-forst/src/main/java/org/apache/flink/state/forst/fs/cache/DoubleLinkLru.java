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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A double link LRU cache.
 */
public abstract class DoubleLinkLru {

    private static final Logger LOG = LoggerFactory.getLogger(DoubleLinkLru.class);

    LruHashMap firstLink;
    LruHashMap secondLink;
    HashMap<String, FileCacheEntry> outOfCache;

    AtomicInteger secondLinkCountLimit = new AtomicInteger(0);

    /** Internal underlying data map. */
    abstract class LruHashMap extends LinkedHashMap<String, FileCacheEntry> {

        private static final int DEFAULT_SIZE = 1024;

        /** Maximum capacity. */
        private final int capacity;

        private final CacheLimitPolicy cacheLimitPolicy;

        LruHashMap(int capacity, CacheLimitPolicy cacheLimitPolicy) {
            super(DEFAULT_SIZE, 0.75f, true);
            this.capacity = capacity;
            this.cacheLimitPolicy = cacheLimitPolicy;
        }

        public boolean isSafeToAdd(FileCacheEntry value) {
            return cacheLimitPolicy.isSafeToAdd(getValueResource(value));
        }

        @Override
        public FileCacheEntry put(String key, FileCacheEntry value) {
            FileCacheEntry previous = super.put(key, value);
            if (previous != null) {
                cacheLimitPolicy.release(getValueResource(previous));
            }
            tryEvict(getValueResource(value));
            cacheLimitPolicy.acquire(getValueResource(value));
            return previous;
        }

        public FileCacheEntry remove(String key) {
            FileCacheEntry value = super.remove(key);
            if (value != null) {
                cacheLimitPolicy.release(getValueResource(value));
            }
            return value;
        }

        /**
         * Try to evict the old entries in cache until the current occupied resource is less than the
         * resource.
         */
        private void tryEvict(long toAddResource) {
            if (!cacheLimitPolicy.isOverflow(toAddResource)) {
                return;
            }
            Iterator<Map.Entry<String, FileCacheEntry>> iterator = entrySet().iterator();
            while (cacheLimitPolicy.isOverflow(toAddResource) && iterator.hasNext()) {
                Map.Entry<String, FileCacheEntry> toRemove = iterator.next();
                if (directRemoveEntry(toRemove)) {
                    iterator.remove();
                }
            }
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, FileCacheEntry> entry) {
            if (capacity > 0 && size() > capacity) {
                internalRemove(entry.getValue());
                cacheLimitPolicy.release(getValueResource(entry.getValue()));
                return true;
            }
            return false;
        }

        private boolean directRemoveEntry(Map.Entry<String, FileCacheEntry> entry) {
            if (size() > 0) {
                internalRemove(entry.getValue());
                cacheLimitPolicy.release(getValueResource(entry.getValue()));
                return true;
            }
            return false;
        }

        abstract void internalRemove(FileCacheEntry value);
    }

    public DoubleLinkLru(CacheLimitPolicy cacheLimitPolicy) {
        firstLink = new LruHashMap(LruHashMap.DEFAULT_SIZE, cacheLimitPolicy) {
            @Override
            void internalRemove(FileCacheEntry value) {
                updateSecondLinkCountLimit(1);
                if (secondLink.isSafeToAdd(value)) {
                    secondLink.put(value.cacheKey, value);
                    addToSecondLink(value);
                } else {
                    addToSecondLink(value);
                    outOfCache.put(value.cacheKey, value);
                    moveOutOfCache(value);
                }
            }
        };
        CacheLimitPolicy secondCacheLimitPolicy = new SizeBasedFloatCountCacheLimitPolicy(
                512 * 1024 * 1024, secondLinkCountLimit
        );
        secondLink = new LruHashMap(LruHashMap.DEFAULT_SIZE, secondCacheLimitPolicy) {
            @Override
            void internalRemove(FileCacheEntry value) {
                outOfCache.put(value.cacheKey, value);
                moveOutOfCache(value);
            }
        };
        outOfCache = new HashMap<>();
        updateSecondLinkCountLimit(0);
    }

    private synchronized void updateSecondLinkCountLimit(int delta) {
        int newValue = (secondLink.size() + outOfCache.size() + delta) / 2 + 1;
        LOG.trace("Update second link count limit from {} to {}.", secondLinkCountLimit.get(), newValue);
        secondLinkCountLimit.set(newValue);
    }

    public synchronized boolean put(String key, FileCacheEntry value) {
        if (firstLink.isSafeToAdd(value)) {
            firstLink.put(key, value);
            addToFirstLink(value);
            return true;
        } else {
            return false;
        }
    }

    public synchronized FileCacheEntry get(String key) {
        FileCacheEntry value = firstLink.get(key);
        if (value == null) {
            value = secondLink.get(key);
            if (value != null) {
                if (whetherToPromoteToFirstLink(value)) {
                    secondLink.remove(key);
                    updateSecondLinkCountLimit(0);
                    firstLink.put(key, value);
                    addToFirstLink(value);
                }
            } else {
                // out of cache
                value = outOfCache.remove(key);
                if (value != null) {
                    if (secondLink.isSafeToAdd(value)) {
                        secondLink.put(key, value);
                        addToSecondLink(value);
                    } else {
                        value = outOfCache.put(key, value);
                    }
                }
            }
        }
        return value;
    }

    public synchronized FileCacheEntry remove(String key) {
        FileCacheEntry value = firstLink.remove(key);
        if (value == null) {
            value = secondLink.remove(key);
            if (value == null) {
                value = outOfCache.remove(key);
            }
        }
        if (value != null) {
            internalRemove(value);
        }
        updateSecondLinkCountLimit(0);
        return value;
    }

    abstract long getValueResource(FileCacheEntry value);

    abstract void addToSecondLink(FileCacheEntry value);

    abstract void addToFirstLink(FileCacheEntry value);

    abstract void internalRemove(FileCacheEntry value);

    abstract boolean whetherToPromoteToFirstLink(FileCacheEntry value);

    abstract void moveOutOfCache(FileCacheEntry value);
}
