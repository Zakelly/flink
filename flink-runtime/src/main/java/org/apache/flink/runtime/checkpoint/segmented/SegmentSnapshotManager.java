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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.filesystem.FsSegmentCheckpointStateOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** Manages meta information in TM side when taking segmented checkpoints. */
public interface SegmentSnapshotManager extends Closeable {

    boolean isEnabled();

    void initFileSystem(SegmentCheckpointUtils.SegmentSnapshotFileSystemInfo fileSystemInfo)
            throws IOException;

    void addSubtask(SubtaskKey subtaskKey);

    /** Restore the file mapping information from the state handles of operator states. */
    void addOperatorStateHandles(
            SubtaskKey subtaskKey,
            long checkpointId,
            List<StateObjectCollection<OperatorStateHandle>> stateHandles);

    /** Restore the file mapping information from the state handles of keyed states. */
    void addKeyedStateHandles(
            SubtaskKey subtaskKey,
            long checkpointId,
            List<StateObjectCollection<KeyedStateHandle>> stateHandles);

    /**
     * Create a new {@link FsSegmentCheckpointStateOutputStream}. Multiple streams may write to the
     * same physical files. When a stream is closed, it returns a {@link SegmentFileStateHandle}
     * that can retrieve the state back.
     *
     * @param checkpointId ID of the checkpoint.
     * @param scope The state's scope, whether it is exclusive or shared.
     * @return An output stream that writes state for the given checkpoint.
     */
    FsSegmentCheckpointStateOutputStream createCheckpointStateOutputStream(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope);

    /** Return the working directory of the segment snapshot manager. */
    Path getSsmManagedDir(SubtaskKey subtaskKey, CheckpointedStateScope scope);

    void notifyCheckpointComplete(SubtaskKey subtaskKey, long checkpointId) throws Exception;

    void notifyCheckpointAborted(SubtaskKey subtaskKey, long checkpointId) throws Exception;

    void notifyCheckpointSubsumed(SubtaskKey subtaskKey, long checkpointId) throws Exception;

    /** A key identifies a subtask. */
    final class SubtaskKey {
        final String taskName;
        final int subtaskIndex;
        final int parallelism;

        final int hashCode;

        private SubtaskKey(TaskInfo taskInfo) {
            this.taskName = taskInfo.getTaskName();
            this.subtaskIndex = taskInfo.getIndexOfThisSubtask();
            this.parallelism = taskInfo.getNumberOfParallelSubtasks();
            int hash = taskName.hashCode();
            hash = 31 * hash + subtaskIndex;
            hash = 31 * hash + parallelism;
            this.hashCode = hash;
        }

        public static SubtaskKey of(TaskInfo taskInfo) {
            return new SubtaskKey(taskInfo);
        }

        public String getManagedDirName() {
            return String.format("%s_%d_%d_", taskName, subtaskIndex, parallelism)
                    .replaceAll("[^a-zA-Z0-9\\-]", "_");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SubtaskKey that = (SubtaskKey) o;

            return hashCode == that.hashCode
                    && subtaskIndex == that.subtaskIndex
                    && parallelism == that.parallelism
                    && taskName.equals(that.taskName);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return String.format("%s(%d/%d)", taskName, subtaskIndex, parallelism);
        }
    }
}
