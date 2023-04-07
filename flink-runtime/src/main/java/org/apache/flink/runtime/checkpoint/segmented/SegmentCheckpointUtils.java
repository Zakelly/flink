/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.segmented;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.OperatorStateHandle;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_ACROSS_BOUNDARY;
import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.SEGMENTED_WITHIN_BOUNDARY;
import static org.apache.flink.runtime.checkpoint.segmented.SegmentType.UNSEGMENTED;

/** Utilities for segmented checkpoints. */
public class SegmentCheckpointUtils {

    // ------------------------------------------------------------------------
    //  SSM initialization
    // ------------------------------------------------------------------------

    /** A class that packs the file system info for snapshot. */
    public static class SegmentSnapshotFileSystemInfo {
        FileSystem fs;
        Path checkpointBaseDirectory;
        Path sharedStateDirectory;
        Path taskOwnedStateDirectory;
        @Nullable Path defaultSavepointDirectory;
        int fileSizeThreshold;
        int writeBufferSize;

        public SegmentSnapshotFileSystemInfo(
                FileSystem fs,
                Path checkpointBaseDirectory,
                @Nullable Path sharedStateDirectory,
                Path taskOwnedStateDirectory,
                @Nullable Path defaultSavepointDirectory,
                int fileSizeThreshold,
                int writeBufferSize) {
            this.fs = fs;
            this.checkpointBaseDirectory = checkpointBaseDirectory;
            this.sharedStateDirectory = sharedStateDirectory;
            this.taskOwnedStateDirectory = taskOwnedStateDirectory;
            this.defaultSavepointDirectory = sharedStateDirectory;
            this.fileSizeThreshold = fileSizeThreshold;
            this.writeBufferSize = writeBufferSize;
        }
    }

    public static SegmentSnapshotManager createSegmentSnapshotManager(
            String id, Configuration configuration) {
        boolean enabled =
                configuration.getBoolean(CheckpointingOptions.ENABLE_SEGMENTED_CHECKPOINT);
        boolean across = configuration.getBoolean(CheckpointingOptions.SEGMENTED_ACROSS_BOUNDARY);
        long maxFileSize =
                configuration.getLong(CheckpointingOptions.SEGMENTED_CHECKPOINT_MAX_FILE_SIZE);
        return createSegmentSnapshotManager(id, enabled, across, maxFileSize, true);
    }

    public static SegmentSnapshotManager createSegmentSnapshotManager(
            String id,
            boolean segmentedEnabled,
            boolean segmentedAcrossCheckpointBoundary,
            long maxFileSize,
            boolean asyncIO) {
        return createSegmentSnapshotManager(
                id,
                getSegmentTypeFromConf(segmentedEnabled, segmentedAcrossCheckpointBoundary),
                maxFileSize,
                asyncIO);
    }

    public static SegmentSnapshotFileSystemInfo packFileSystemInfo(
            FileSystem fs,
            Path checkpointBaseDirectory,
            Path sharedStateDirectory,
            Path taskOwnedStateDirectory,
            @Nullable Path defaultSavepointDirectory,
            int fileSizeThreshold,
            int writeBufferSize) {
        return new SegmentSnapshotFileSystemInfo(
                fs,
                checkpointBaseDirectory,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                defaultSavepointDirectory,
                fileSizeThreshold,
                writeBufferSize);
    }

    public static SegmentSnapshotManager createSegmentSnapshotManager(
            String id, SegmentType segmentType, long maxFileSize, boolean asyncIO) {
        Executor ioExecutor = asyncIO ? Executors.newCachedThreadPool() : Runnable::run;
        switch (segmentType) {
            case SEGMENTED_WITHIN_BOUNDARY:
                return new WithinBoundarySegmentSnapshotManager(
                        id, segmentType, maxFileSize, ioExecutor);
            case SEGMENTED_ACROSS_BOUNDARY:
                return new AcrossBoundarySegmentSnapshotManager(
                        id, segmentType, maxFileSize, ioExecutor);
            case UNSEGMENTED:
            default:
                return null;
        }
    }

    public static SegmentType getSegmentTypeFromConf(
            boolean segmentedEnabled, boolean segmentedAcrossCheckpointBoundary) {
        if (!segmentedEnabled) {
            return UNSEGMENTED;
        } else {
            return segmentedAcrossCheckpointBoundary
                    ? SEGMENTED_ACROSS_BOUNDARY
                    : SEGMENTED_WITHIN_BOUNDARY;
        }
    }

    // ------------------------------------------------------------------------
    //  SSM initialization
    // ------------------------------------------------------------------------

    public static SegmentOperatorStreamStateHandle emptyOperatorStreamStateHandle(
            Path ssmManagedDirectory) {
        final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
                Collections.emptyMap();
        return new EmptySegmentOperatorStreamStateHandle(
                new File(ssmManagedDirectory.getPath()).toPath(),
                writtenStatesMetaData,
                getEmptySegmentFileStateHandle());
    }

    public static SegmentFileStateHandle getEmptySegmentFileStateHandle() {
        return EmptySegmentFileStateHandle.INSTANCE;
    }
}
