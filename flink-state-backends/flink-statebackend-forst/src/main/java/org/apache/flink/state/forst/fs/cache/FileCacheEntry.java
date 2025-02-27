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
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A file cache entry that encapsulates file and the size of the file, and provides methods to read
 * or write file. Not thread safe.
 */
public class FileCacheEntry extends ReferenceCounted {
    private static final Logger LOG = LoggerFactory.getLogger(FileCacheEntry.class);
    private static final int READ_BUFFER_SIZE = 64 * 1024;

    final FileBasedCache fileBasedCache;

    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The original path of file. */
    final Path originalPath;

    /** The path in cache. */
    final Path cachePath;

    /** The size of file. */
    final long entrySize;

    volatile boolean closed;

    final Queue<CachedDataInputStream> openedStreams;

    final String cacheKey;

    final AtomicReference<EntryStatus> status;

    Runnable touchFunction;

    long secondAccessEpoch = 0L;

    int promoteCount = 0;

    int evictCount = 0;

    public enum EntryStatus {
        LOADED,
        LOADING,
        INVALID,
        REMOVING,
        REMOVED,
        CLOSING,
        CLOSED
    }

    FileCacheEntry(
            FileBasedCache fileBasedCache, Path originalPath, Path cachePath, long entrySize) {
        super(0);
        this.fileBasedCache = fileBasedCache;
        this.cacheFs = fileBasedCache.cacheFs;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.entrySize = entrySize;
        this.closed = false;
        this.openedStreams = new LinkedBlockingQueue<>();
        this.cacheKey = cachePath.toString();
        this.status = new AtomicReference<>(EntryStatus.REMOVED);
        LOG.trace("Create new cache entry {}.", cachePath);
    }

    public CachedDataInputStream open(FSDataInputStream originalStream) throws IOException {
        LOG.trace("Open new stream for cache entry {}.", cachePath);
        FSDataInputStream cacheStream = getCacheStream();
        if (cacheStream != null) {
            CachedDataInputStream inputStream =
                    new CachedDataInputStream(fileBasedCache, this, cacheStream, originalStream);
            openedStreams.add(inputStream);
            release();
            return inputStream;
        } else {
            CachedDataInputStream inputStream =
                    new CachedDataInputStream(fileBasedCache, this, originalStream);
            openedStreams.add(inputStream);
            return inputStream;
        }
    }

    public FSDataInputStream getCacheStream() throws IOException {
        if (status.get() == EntryStatus.LOADED && tryRetain() > 0) {
            return cacheFs.open(cachePath);
        }
        return null;
    }

    public void touch() {
        touchFunction.run();
    }

    public Path load() {
        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
             final byte[] buffer = new byte[READ_BUFFER_SIZE];

             inputStream = originalPath.getFileSystem().open(originalPath, READ_BUFFER_SIZE);

             outputStream = cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE);

             long maxTransferBytes = originalPath.getFileSystem().getFileStatus(originalPath).getLen();

             while (maxTransferBytes > 0) {
                 int maxReadBytes = (int) Math.min(maxTransferBytes, READ_BUFFER_SIZE);
                 int readBytes = inputStream.read(buffer, 0, maxReadBytes);

                 if (readBytes == -1) {
                     break;
                 }

                 outputStream.write(buffer, 0, readBytes);

                 maxTransferBytes -= readBytes;
             }
             return cachePath;
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public synchronized void loaded() {
        // 0 -> 1
        if (status.get() == EntryStatus.LOADED) {
            retain();
        }
    }

    public synchronized boolean invalidate() {
        if (switchStatus(EntryStatus.LOADED, EntryStatus.INVALID)) {
            release();
            return true;
        }
        return false;
    }

    public synchronized void invalidateOnClose() {
        release();
    }

    public synchronized void close() {
        if (getAndSetStatus(EntryStatus.CLOSING) == EntryStatus.LOADED) {
            fileBasedCache.scheduleRemove(this);
        } else {
            status.set(EntryStatus.CLOSED);
        }
    }

    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        fileBasedCache.removeFile(this);
    }

    public void doRemoveFile() {
        try {
            Iterator<CachedDataInputStream> iterator = openedStreams.iterator();
            while (iterator.hasNext()) {
                CachedDataInputStream stream = iterator.next();
                if (stream.isClosed()) {
                    iterator.remove();
                } else {
                    stream.closeCachedStream();
                }
            }
            cacheFs.delete(cachePath, false);
            if (status.get() != EntryStatus.CLOSED) {
                status.set(FileCacheEntry.EntryStatus.REMOVED);
            }
        } catch (Exception e) {
            LOG.warn("Failed to delete cache entry {}.", cachePath, e);
        }
    }

    public boolean switchStatus(EntryStatus from, EntryStatus to) {
        if (status.compareAndSet(from, to)) {
            LOG.trace(
                    "Cache {} (for {}) Switch status from {} to {}.",
                    originalPath,
                    cachePath,
                    from,
                    to);
            return true;
        } else {
            return false;
        }
    }

    public EntryStatus getAndSetStatus(EntryStatus to) {
        return status.getAndSet(to);
    }
}
