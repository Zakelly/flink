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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManager;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManagerHolder;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/** FsSegmentCheckpointStreamFactory. */
public class FsSegmentCheckpointStorageLocation extends FsCheckpointStorageLocation
        implements SegmentSnapshotManagerHolder {

    private final SegmentSnapshotManager segmentSnapshotManager;

    private final long checkpointId;

    private final Supplier<FsCheckpointStorageLocation> backwardsConvertor;

    private final SegmentSnapshotManager.SubtaskKey subtaskKey;

    public FsSegmentCheckpointStorageLocation(
            SegmentSnapshotManager.SubtaskKey subtaskKey,
            FileSystem fileSystem,
            Path checkpointDir,
            Path sharedStateDir,
            Path taskOwnedStateDir,
            CheckpointStorageLocationReference reference,
            int fileStateSizeThreshold,
            int writeBufferSize,
            SegmentSnapshotManager segmentSnapshotManager,
            long checkpointId) {
        super(
                fileSystem,
                checkpointDir,
                sharedStateDir,
                taskOwnedStateDir,
                reference,
                fileStateSizeThreshold,
                writeBufferSize);

        this.subtaskKey = subtaskKey;
        this.checkpointId = checkpointId;
        this.segmentSnapshotManager = segmentSnapshotManager;
        backwardsConvertor =
                () ->
                        new FsCheckpointStorageLocation(
                                fileSystem,
                                checkpointDir,
                                sharedStateDir,
                                taskOwnedStateDir,
                                reference,
                                fileStateSizeThreshold,
                                writeBufferSize);
    }

    public CheckpointStreamFactory toUnsegmented() {
        return backwardsConvertor.get();
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle stateHandle, CheckpointedStateScope scope)
            throws IOException {
        return false;
    }

    @Override
    public List<StreamStateHandle> duplicate(
            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) throws IOException {
        return null;
    }

    @Override
    public FsSegmentCheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) throws IOException {
        return segmentSnapshotManager.createCheckpointStateOutputStream(
                subtaskKey, checkpointId, scope);
    }

    @VisibleForTesting
    SegmentSnapshotManager getSegmentSnapshotManager() {
        return segmentSnapshotManager;
    }

    @Override
    public Path getSsmWorkingDirectory(CheckpointedStateScope scope) {
        return segmentSnapshotManager.getSsmManagedDir(subtaskKey, scope);
    }
}
