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
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringBasedID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;

/** An abstraction of logical files in segmented checkpoints. */
public class LogicalFile {

    /** ID for {@link LogicalFile}. It should be unique for each file. */
    public static class LogicalFileId extends StringBasedID {

        public LogicalFileId(String keyString) {
            super(keyString);
        }

        public Path getFilePath() {
            return new Path(getKeyString());
        }

        public static LogicalFileId generateRandomID() {
            return new LogicalFileId(UUID.randomUUID().toString());
        }
    }

    LogicalFileId fileID;

    private long lastCheckpointId;

    boolean isRemoved = false;

    /**
     * FileID of the physical file where this logical file is stored. This should be null until a
     * physical file is assigned to this logical file.
     */
    @Nonnull private final PhysicalFile physicalFile;

    @Nonnull private final SegmentSnapshotManager.SubtaskKey subtaskKey;

    public static LogicalFile getRestoredInstance(
            LogicalFileId fileID,
            @Nonnull PhysicalFile physicalFile,
            @Nonnull SegmentSnapshotManager.SubtaskKey subtaskKey) {
        return new LogicalFile(fileID, physicalFile, subtaskKey);
    }

    public LogicalFile(
            LogicalFileId fileID,
            @Nonnull PhysicalFile physicalFile,
            @Nonnull SegmentSnapshotManager.SubtaskKey subtaskKey) {
        this.fileID = fileID;
        this.physicalFile = physicalFile;
        this.subtaskKey = subtaskKey;
        physicalFile.incRefCount();
    }

    public LogicalFileId getFileID() {
        return fileID;
    }

    public void discardWithCheckpointID(long checkpointID, FileOperationReason reason)
            throws IOException {
        if (!isRemoved) {
            physicalFile.decRefCount(reason);
            isRemoved = true;
        }
    }

    public void advanceLastCheckpointId(long checkpointID) {
        this.lastCheckpointId = checkpointID;
    }

    public long getLastCheckpointId() {
        return lastCheckpointId;
    }

    @Nullable
    public PhysicalFile getPhysicalFile() {
        return physicalFile;
    }

    @Nonnull
    public SegmentSnapshotManager.SubtaskKey getSubtaskKey() {
        return subtaskKey;
    }

    @Override
    public int hashCode() {
        return fileID.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogicalFile that = (LogicalFile) o;
        return fileID.equals(that.fileID);
    }

    @VisibleForTesting
    public boolean identicalTo(LogicalFile that) {
        return fileID.equals(that.fileID) && physicalFile.equals(that.physicalFile);
    }
}
