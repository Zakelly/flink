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
import java.util.concurrent.Executor;

/**
 * A {@link SegmentSnapshotManager} that works in {@link SegmentType#SEGMENTED_ACROSS_BOUNDARY}
 * mode.
 */
public class AcrossBoundarySegmentSnapshotManager extends SegmentSnapshotManagerBase {

    private final PhysicalFilePool filePool;

    public AcrossBoundarySegmentSnapshotManager(
            String id, SegmentType segmentType, long maxFileSize, Executor ioExecutor) {
        super(id, segmentType, maxFileSize, ioExecutor);
        filePool = new PhysicalFilePool(PhysicalFilePool.Mode.NULL_WHEN_NO_FILE);
    }

    // ------------------------------------------------------------------------
    //  CheckpointListener
    // ------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  restore
    // ------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  snapshot
    // ------------------------------------------------------------------------

    @Override
    @Nonnull
    protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointID, CheckpointedStateScope scope)
            throws IOException {
        PhysicalFile result = filePool.pollFile(subtaskKey, scope);

        // a new file could be put into the file pool after closeAndGetHandle()
        return result == null ? createPhysicalFile(subtaskKey, scope) : result;
    }

    @Override
    protected void endCheckpointOfAllSubtasks(long checkpointId) {}

    @Override
    protected void tryReusingPhysicalFileAfterClosingStream(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile)
            throws IOException {

        if (physicalFile.getSize() >= physicalFileSizeThreshold) {
            // If the physical file size has reached the limit, we close the file and do not return
            // it to the file pool
            physicalFile.close();
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
}
