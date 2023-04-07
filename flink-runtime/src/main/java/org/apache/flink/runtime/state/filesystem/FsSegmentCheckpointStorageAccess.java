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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.segmented.SegmentCheckpointUtils;
import org.apache.flink.runtime.checkpoint.segmented.SegmentSnapshotManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

/** An implementation of segmented checkpoint storage to file systems. */
public class FsSegmentCheckpointStorageAccess extends FsCheckpointStorageAccess {

    private final SegmentSnapshotManager segmentSnapshotManager;

    private final SegmentSnapshotManager.SubtaskKey subtaskKey;

    public FsSegmentCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            Path sharedStateDirectory,
            Path taskOwnedStateDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize,
            SegmentSnapshotManager segmentSnapshotManager,
            Environment environment)
            throws IOException {
        super(
                fs,
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
        this.segmentSnapshotManager = segmentSnapshotManager;
        this.subtaskKey = SegmentSnapshotManager.SubtaskKey.of(environment.getTaskInfo());
        initSegmentSnapshotManager(
                fs,
                checkpointBaseDirectory,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                defaultSavepointDirectory,
                fileSizeThreshold,
                writeBufferSize);
    }

    private void initSegmentSnapshotManager(
            FileSystem fs,
            Path checkpointBaseDirectory,
            Path sharedStateDirectory,
            Path taskOwnedStateDirectory,
            @Nullable Path defaultSavepointDirectory,
            int fileSizeThreshold,
            int writeBufferSize) {
        try {
            segmentSnapshotManager.initFileSystem(
                    new SegmentCheckpointUtils.SegmentSnapshotFileSystemInfo(
                            fs,
                            checkpointBaseDirectory,
                            sharedStateDirectory,
                            taskOwnedStateDirectory,
                            defaultSavepointDirectory,
                            fileSizeThreshold,
                            writeBufferSize));

            segmentSnapshotManager.addSubtask(subtaskKey);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SegmentSnapshotManager getSegmentSnapshotManager() {
        return segmentSnapshotManager;
    }

    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference, boolean isSavepoint)
            throws IOException {
        if (isSavepoint) {
            return super.resolveCheckpointStorageLocation(checkpointId, reference);
        } else {
            return resolveCheckpointStorageLocation(checkpointId, reference);
        }
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException {

        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir =
                    createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsSegmentCheckpointStorageLocation(
                    subtaskKey,
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    segmentSnapshotManager,
                    checkpointId);
        } else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsSegmentCheckpointStorageLocation(
                    subtaskKey,
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    segmentSnapshotManager,
                    checkpointId);
        }
    }
}
