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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A file-granularity LRU cache. Only newly generated SSTs are written to the cache, the file
 * reading from the remote will not. Newly generated SSTs are written to the original file system
 * and cache simultaneously, so, the cached file can be directly deleted with persisting when
 * evicting.
 */
public final class FileBasedCache extends DoubleLinkedLru<String, FileCacheEntry> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedCache.class);

    private static final String FORST_CACHE_PREFIX = "forst.fileCache";

    private static final ThreadLocal<Boolean> isFlinkThread = ThreadLocal.withInitial(() -> false);

    private final CacheLimitPolicy cacheLimitPolicy;

    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The base path of cache. */
    private final Path basePath;

    /** Whether the cache is closed. */
    private volatile boolean closed;

    private final ExecutorService executorService;

    private final ScheduledExecutorService scheduledExecutorService;

    /** Hit metric. */
    private transient Counter hitCounter;

    /** Miss metric. */
    private transient Counter missCounter;

    /** Metric for load back */
    private transient Counter loadBackCounter;

    /** Metric for eviction */
    private transient Counter evictCounter;

    private long secondAccessEpoch = 0L;

    public FileBasedCache(
            int capacity,
            CacheLimitPolicy cacheLimitPolicy,
            FileSystem cacheFs,
            Path basePath,
            MetricGroup metricGroup) {
        this.closed = false;
        this.cacheLimitPolicy = cacheLimitPolicy;
        this.cacheFs = cacheFs;
        this.basePath = basePath;
        this.executorService =
                Executors.newFixedThreadPool(
                        4,
                        new ExecutorThreadFactory("ForSt-LruLoader"));
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1,
                new ExecutorThreadFactory("ForSt-Lru-Disposer"));
        if (metricGroup != null) {
            this.hitCounter =
                    metricGroup.counter(FORST_CACHE_PREFIX + ".hit", new ThreadSafeSimpleCounter());
            this.missCounter =
                    metricGroup.counter(
                            FORST_CACHE_PREFIX + ".miss", new ThreadSafeSimpleCounter());
            this.loadBackCounter =
                    metricGroup.counter(FORST_CACHE_PREFIX + ".loadback", new ThreadSafeSimpleCounter());
            this.evictCounter =
                    metricGroup.counter(
                            FORST_CACHE_PREFIX + ".evict", new ThreadSafeSimpleCounter());
            metricGroup.gauge(
                    FORST_CACHE_PREFIX + ".usedBytes", () -> cacheLimitPolicy.usedBytes());
            cacheLimitPolicy.registerCustomizedMetrics(FORST_CACHE_PREFIX, metricGroup);
        }
        LOG.info(
                "FileBasedCache initialized, basePath: {}, cache limit policy: {}",
                basePath,
                cacheLimitPolicy);
    }

    public static void setFlinkThread() {
        isFlinkThread.set(true);
    }

    public static boolean isFlinkThread() {
        return isFlinkThread.get();
    }

    public void incHitCounter() {
        if (hitCounter != null && isFlinkThread.get()) {
            hitCounter.inc();
        }
    }

    public void incMissCounter() {
        if (missCounter != null && isFlinkThread.get()) {
            missCounter.inc();
        }
    }

    Path getCachePath(Path fromOriginal) {
        return new Path(basePath, fromOriginal.getName());
    }

    public CachedDataInputStream open(Path path, FSDataInputStream originalStream)
            throws IOException {
        if (closed) {
            return null;
        }
        FileCacheEntry entry = get(getCachePath(path).toString(), isFlinkThread());
        if (entry != null) {
            return entry.open(originalStream);
        } else {
            return null;
        }
    }

    public CachedDataOutputStream create(FSDataOutputStream originalOutputStream, Path path)
            throws IOException {
        if (closed) {
            return null;
        }
        Path cachePath = getCachePath(path);
        return new CachedDataOutputStream(
                path,
                cachePath,
                originalOutputStream,
                cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE),
                this);
    }

    public void delete(Path path) {
        if (!closed) {
            remove(getCachePath(path).toString());
        }
    }

    //-----------------------------
    // Overriding methods to provide thread-safe
    //-----------------------------

    @Override
    public FileCacheEntry get(String key, boolean affectOrder) {
        synchronized (this) {
            return super.get(key, affectOrder);
        }
    }


    @Override
    public void addFirst(String key, FileCacheEntry value) {
        synchronized (this) {
            super.addFirst(key, value);
        }
    }

    @Override
    public void addSecond(String key, FileCacheEntry value) {
        synchronized (this) {
            super.addSecond(key, value);
        }
    }

    @Override
    public FileCacheEntry remove(String key) {
        synchronized (this) {
            return super.remove(key);
        }
    }

    /**
     * Directly insert in cache when restoring.
     */
    public void registerInCache(Path originalPath, long size) {
        Path cachePath = getCachePath(originalPath);
        FileCacheEntry fileCacheEntry =
                new FileCacheEntry(this, originalPath, cachePath, size);
        fileCacheEntry.promoteCount = 5;
        addSecond(cachePath.toString(), fileCacheEntry);
    }

    public void removeFile(FileCacheEntry entry) {
        if (entry.switchStatus(FileCacheEntry.EntryStatus.INVALID, FileCacheEntry.EntryStatus.REMOVING)
        || entry.switchStatus(FileCacheEntry.EntryStatus.CLOSING, FileCacheEntry.EntryStatus.CLOSED)) {
            executorService.submit(entry::doRemoveFile);
        }
    }

    public void scheduleRemove(FileCacheEntry entry) {
        scheduledExecutorService.schedule(entry::invalidateOnClose, 2000, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }

    //-----------------------------
    // Hook methods implementation
    //-----------------------------

    @Override
    boolean isSafeToAddFirst(FileCacheEntry value) {
        return cacheLimitPolicy.isSafeToAdd(value.entrySize);
    }

    @Override
    void newNodeCreated(FileCacheEntry value, DoubleLinkedLru<String, FileCacheEntry>.Node n) {
        value.touchFunction = () -> {
            synchronized (FileBasedCache.this) {
                accessNode(n);
            }
        };
    }

    @Override
    void addedToFirst(FileCacheEntry value) {
        LOG.info("Cache entry {} added to first link.", value.cachePath.getName());
        while(cacheLimitPolicy.isOverflow(value.entrySize)) {
            moveMiddleFront();
        }
        cacheLimitPolicy.acquire(value.entrySize);
    }

    @Override
    void addedToSecond(FileCacheEntry value) {
        LOG.info("Cache entry {} added to second link.", value.cachePath.getName());
        value.secondAccessEpoch = (++secondAccessEpoch);
    }

    @Override
    void removedFromFirst(FileCacheEntry value) {
        cacheLimitPolicy.release(value.entrySize);
        value.close();
    }

    @Override
    void removedFromSecond(FileCacheEntry value) {
        value.close();
    }

    @Override
    void movedToFirst(FileCacheEntry entry) {
        // here we won't consider the cache limit policy.
        // since there will be promotedToFirst called after this.
        LOG.info("Cache entry {} moved to first link.", entry.cachePath.getName());
        // trigger the loading
        if (entry.switchStatus(FileCacheEntry.EntryStatus.INVALID, FileCacheEntry.EntryStatus.LOADED)) {
            // just a try
            entry.loaded();
            if (loadBackCounter != null) {
                loadBackCounter.inc();
            }
        }  if (entry.switchStatus(FileCacheEntry.EntryStatus.REMOVED, FileCacheEntry.EntryStatus.LOADING)) {
            executorService.submit(() -> {
                if (entry.status.get() == FileCacheEntry.EntryStatus.LOADING) {
                    Path path = entry.load();
                    if (path == null) {
                        entry.switchStatus(
                                FileCacheEntry.EntryStatus.LOADING,
                                FileCacheEntry.EntryStatus.REMOVED);
                    } else if (entry.switchStatus(
                            FileCacheEntry.EntryStatus.LOADING,
                            FileCacheEntry.EntryStatus.LOADED)) {
                        entry.loaded();
                        if (loadBackCounter != null) {
                            loadBackCounter.inc();
                        }
                    } else {
                        try {
                            path.getFileSystem().delete(path, false);
                            // delete the file
                        } catch (IOException e) {
                        }
                    }
                }
            });
        }
    }

    @Override
    void movedToSecond(FileCacheEntry value) {
        // trigger the evicting
        LOG.info("Cache entry {} moved to second link.", value.cachePath.getName());
        cacheLimitPolicy.release(value.entrySize);
        if (value.invalidate() && evictCounter != null) {
            evictCounter.inc();
            value.evictCount++;
        }
    }

    @Override
    boolean nodeAccessedAtSecond(FileCacheEntry value) {
        if (secondAccessEpoch - value.secondAccessEpoch < getSecondSize() / 2) {
            // current entry are still under the second link limit
            value.promoteCount++;
        } else {
            value.promoteCount = 0;
            secondAccessEpoch++;
        }
        value.secondAccessEpoch = secondAccessEpoch;
        return value.evictCount < 3 && ++value.promoteCount > 5;
    }

    @Override
    void promotedToFirst(FileCacheEntry value) {
        value.promoteCount = 0;
        while(cacheLimitPolicy.isOverflow(value.entrySize)) {
            moveMiddleFront();
        }
        cacheLimitPolicy.acquire(value.entrySize);
    }
}
