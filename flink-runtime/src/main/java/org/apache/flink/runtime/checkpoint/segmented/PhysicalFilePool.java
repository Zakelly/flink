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

import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** A pool for physical file reusing. Thread-safe. */
public class PhysicalFilePool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalFilePool.class);

    /** The policy when there is no file. */
    public enum Mode {
        NULL_WHEN_NO_FILE,
        BLOCK_WHEN_NO_FILE
    }

    private final Map<SegmentSnapshotManager.SubtaskKey, Queue<PhysicalFile>>
            sharedPhysicalFilePoolBySubtask;

    private final Queue<PhysicalFile> exclusivePhysicalFilePool;

    private boolean exclusivePhysicalFilePoolInitialized = false;

    private final Mode mode;

    public PhysicalFilePool(Mode mode) {
        this.mode = mode;
        this.sharedPhysicalFilePoolBySubtask = new ConcurrentHashMap<>();
        this.exclusivePhysicalFilePool = createFileQueue();
    }

    private Queue<PhysicalFile> createFileQueue() {
        if (mode == Mode.NULL_WHEN_NO_FILE) {
            return new ConcurrentLinkedQueue<>();
        } else {
            // blocking
            return new LinkedBlockingQueue<>();
        }
    }

    private Queue<PhysicalFile> getFileQueue(
            SegmentSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            Queue<PhysicalFile> queue = sharedPhysicalFilePoolBySubtask.get(subtaskKey);
            if (queue == null) {
                LOG.trace("Initializing file queue for scope {} of {}", scope, subtaskKey);
                queue = createFileQueue();
                sharedPhysicalFilePoolBySubtask.put(subtaskKey, queue);
            }
            return queue;
        } else {
            if (!exclusivePhysicalFilePoolInitialized) {
                LOG.trace("Initializing file queue for scope {} of {}", scope, subtaskKey);
                exclusivePhysicalFilePoolInitialized = true;
            }
            return exclusivePhysicalFilePool;
        }
    }

    private void closeFilesInQueue(Queue<PhysicalFile> queue) throws IOException {
        while (!queue.isEmpty()) {
            PhysicalFile physicalFile = queue.poll();
            if (physicalFile != null) {
                physicalFile.close();
            }
        }
    }

    public boolean isInitialized(
            SegmentSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            return sharedPhysicalFilePoolBySubtask.containsKey(subtaskKey);
        } else {
            return exclusivePhysicalFilePoolInitialized;
        }
    }

    @Nullable
    public PhysicalFile pollFile(
            SegmentSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        Queue<PhysicalFile> queue = getFileQueue(subtaskKey, scope);
        if (mode == Mode.BLOCK_WHEN_NO_FILE) {
            try {
                return ((BlockingQueue<PhysicalFile>) queue).take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            return queue.poll();
        }
    }

    public void putFile(SegmentSnapshotManager.SubtaskKey subtaskKey, PhysicalFile physicalFile) {
        getFileQueue(subtaskKey, physicalFile.getScope()).offer(physicalFile);
    }

    public void closeFiles(SegmentSnapshotManager.SubtaskKey subtaskKey) throws IOException {
        Queue<PhysicalFile> queue = sharedPhysicalFilePoolBySubtask.remove(subtaskKey);
        if (queue != null) {
            closeFilesInQueue(queue);
        }
    }

    @Override
    public void close() throws IOException {
        closeFilesInQueue(exclusivePhysicalFilePool);
        for (Queue<PhysicalFile> queue : sharedPhysicalFilePoolBySubtask.values()) {
            closeFilesInQueue(queue);
        }
        sharedPhysicalFilePoolBySubtask.clear();
    }
}
