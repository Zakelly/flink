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

package org.apache.flink.runtime.checkpoint.segmented;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.runtime.checkpoint.segmented.FileOperationReason.FileOperationReasonType.FILE_CLOSE;

/** An abstraction of physical files in segmented checkpoints. */
public class PhysicalFile {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalFile.class);

    /** Functional interface to delete the physical file. */
    @FunctionalInterface
    public interface PhysicalFileDeleter {
        /** Close the outputStream if presented, and delete the file. */
        void perform(
                @Nullable FSDataOutputStream outputStream,
                Path filePath,
                FileOperationReason reason);
    }

    /** Output stream to the file. It can be null if the file is closed. */
    @Nullable private FSDataOutputStream outputStream;

    private final AtomicInteger logicalFileRefCount;

    private final AtomicLong size;

    @Nullable private final PhysicalFileDeleter deleter;

    private final Path filePath;

    private final CheckpointedStateScope scope;

    /**
     * If a physical file is closed, it means no more file segments will be written to the physical
     * file, and it can be deleted once its logicalFileRefCount decreases to 0.
     */
    private boolean closed;

    private boolean isDeleted = false;

    public PhysicalFile(
            @Nullable FSDataOutputStream outputStream,
            Path filePath,
            PhysicalFileDeleter deleter,
            CheckpointedStateScope scope) {
        this.filePath = filePath;
        this.outputStream = outputStream;
        this.closed = outputStream == null;
        this.deleter = deleter;
        this.scope = scope;
        this.size = new AtomicLong(0);
        this.logicalFileRefCount = new AtomicInteger(0);
    }

    public static PhysicalFile getClosedInstance(
            Path filePath, PhysicalFileDeleter deleter, CheckpointedStateScope scope) {
        return new PhysicalFile(null, filePath, deleter, scope);
    }

    @Nullable
    public FSDataOutputStream getOutputStream() {
        return outputStream;
    }

    void incRefCount() {
        int newValue = this.logicalFileRefCount.incrementAndGet();
        LOG.trace(
                "Increase the reference count of physical file: {} by 1. New value is: {}.",
                this.filePath,
                newValue);
    }

    void decRefCount(FileOperationReason reason) throws IOException {
        Preconditions.checkArgument(logicalFileRefCount.get() > 0);
        int newValue = this.logicalFileRefCount.decrementAndGet();
        LOG.trace(
                "Decrease the reference count of physical file: {} by 1. New value is: {}. "
                        + "Reason: {}",
                this.filePath,
                newValue,
                reason);
        deleteIfNecessary(reason, false);
    }

    public void deleteIfNecessary(FileOperationReason reason, boolean forceClose)
            throws IOException {
        if (forceClose) {
            this.close();
        }

        synchronized (this) {
            if (closed && !isDeleted && this.logicalFileRefCount.get() <= 0) {
                if (deleter != null) {
                    deleter.perform(outputStream, filePath, reason);
                }
                this.isDeleted = true;
            }
        }
    }

    void incSize(long delta) {
        this.size.addAndGet(delta);
    }

    long getSize() {
        return size.get();
    }

    @VisibleForTesting
    int getRefCount() {
        return logicalFileRefCount.get();
    }

    public void close() throws IOException {
        interClose();
        deleteIfNecessary(new FileOperationReason(FILE_CLOSE, -1L), false);
    }

    private void interClose() throws IOException {
        closed = true;
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }

    public boolean isOpen() {
        return !closed && outputStream != null;
    }

    public Path getFilePath() {
        return filePath;
    }

    public CheckpointedStateScope getScope() {
        return scope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PhysicalFile that = (PhysicalFile) o;
        return filePath.equals(that.filePath);
    }

    @VisibleForTesting
    public boolean identicalTo(PhysicalFile that) {
        // physical file is forced to be closed when restored, so we skip testing
        // "closed" and "outputStream" here
        return filePath.equals(that.filePath)
                && Objects.equals(logicalFileRefCount, that.logicalFileRefCount)
                && Objects.equals(size, that.size)
                && scope.equals(that.scope);
    }

    @Override
    public String toString() {
        return String.format(
                "Physical File: [%s], closed: %s, logicalFileRefCount: %d",
                filePath, closed, logicalFileRefCount.get());
    }
}
