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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * A {@link SegmentSnapshotManager} that works in {@link SegmentType#SEGMENTED_WITHIN_BOUNDARY}
 * mode.
 */
public class WithinBoundarySegmentSnapshotManager extends SegmentSnapshotManagerBase {
    /**
     * OutputStreams to be reused when writing checkpoint files. For WITHIN_BOUNDARY mode, physical
     * files are NOT shared among multiple checkpoints.
     */
    private final Map<Long, PhysicalFilePool> filePoolByCheckpointId;

    public WithinBoundarySegmentSnapshotManager(
            String id, SegmentType segmentType, long maxFileSize, Executor ioExecutor) {
        // currently there is no file size limit For WITHIN_BOUNDARY mode
        super(id, segmentType, maxFileSize, ioExecutor);
        filePoolByCheckpointId = new ConcurrentHashMap<>();
    }

    // ------------------------------------------------------------------------
    //  CheckpointListener
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        super.notifyCheckpointComplete(subtaskKey, checkpointId);
        removeQueueAneCloseFiles(subtaskKey, checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(SubtaskKey subtaskKey, long checkpointId) throws Exception {
        super.notifyCheckpointAborted(subtaskKey, checkpointId);
        removeQueueAneCloseFiles(subtaskKey, checkpointId);
    }

    private void removeQueueAneCloseFiles(SubtaskKey subtaskKey, long checkpointId)
            throws IOException {
        PhysicalFilePool filePool = filePoolByCheckpointId.get(checkpointId);
        if (filePool != null) {
            filePool.closeFiles(subtaskKey);
        }
    }

    private PhysicalFilePool getOrCreateFilePool(long checkpointId) {
        PhysicalFilePool filePool = filePoolByCheckpointId.get(checkpointId);
        if (filePool == null) {
            filePool = new PhysicalFilePool(PhysicalFilePool.Mode.BLOCK_WHEN_NO_FILE);
            filePoolByCheckpointId.put(checkpointId, filePool);
        }
        return filePool;
    }

    // ------------------------------------------------------------------------
    //  restore
    // ------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  snapshot
    // ------------------------------------------------------------------------

    @Override
    @Nonnull
    protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope) {
        PhysicalFilePool filePool = getOrCreateFilePool(checkpointId);
        if (!filePool.isInitialized(subtaskKey, scope)) {
            try {
                filePool.putFile(subtaskKey, createPhysicalFile(subtaskKey, scope));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return filePool.pollFile(subtaskKey, scope);
    }

    @Override
    protected void tryReusingPhysicalFileAfterClosingStream(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile)
            throws IOException {
        PhysicalFilePool filePool = getOrCreateFilePool(checkpointId);
        if (physicalFile.getSize() >= physicalFileSizeThreshold) {
            // If the physical file size has reached the limit, we close the file and do not return
            // it to the file pool
            physicalFile.close();
            filePool.putFile(subtaskKey, createPhysicalFile(subtaskKey, physicalFile.getScope()));
        } else {
            if (syncAfterClosingLogicalFile) {
                FSDataOutputStream os = physicalFile.getOutputStream();
                if (os != null) {
                    os.flush();
                    os.sync();
                }
            }
            filePool.putFile(subtaskKey, physicalFile);
        }
    }

    @Override
    protected void endCheckpointOfAllSubtasks(long checkpointId) throws IOException {
        PhysicalFilePool filePool = filePoolByCheckpointId.remove(checkpointId);
        if (filePool != null) {
            filePool.close();
        }
    }
}
