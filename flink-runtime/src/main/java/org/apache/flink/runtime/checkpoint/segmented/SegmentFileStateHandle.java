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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsSegmentDataInputStream;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link FileStateHandle} for state that was written to a file segment. A {@link
 * SegmentFileStateHandle} represents a {@link LogicalFile}, which has already been written to a
 * segment in a physical file.
 */
public class SegmentFileStateHandle extends FileStateHandle {

    private static final long serialVersionUID = 1L;

    private final long startPos;
    private final long stateSize;

    private final LogicalFile.LogicalFileId logicalFileId;

    private final CheckpointedStateScope scope;

    /** Creates a new file state for the given file path. */
    public SegmentFileStateHandle(
            Path filePath,
            long startPos,
            long stateSize,
            LogicalFile.LogicalFileId logicalFileId,
            CheckpointedStateScope scope) {
        super(filePath, stateSize);
        this.startPos = startPos;
        this.scope = scope;
        this.stateSize = stateSize;
        this.logicalFileId = logicalFileId;
    }

    /**
     * This method should be empty, so that the JM is not in charge of the lifecycle of files in a
     * segmented checkpoint.
     */
    @Override
    public void discardState() {}

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        FSDataInputStream inputStream = super.openInputStream();
        return new FsSegmentDataInputStream(inputStream, startPos, stateSize);
    }

    public long getStartPos() {
        return startPos;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    public long getEndPos() {
        return getStartPos() + getStateSize();
    }

    public LogicalFile.LogicalFileId getLogicalFileId() {
        return logicalFileId;
    }

    public CheckpointedStateScope getScope() {
        return scope;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SegmentFileStateHandle that = (SegmentFileStateHandle) o;

        return super.equals(that)
                && startPos == that.startPos
                && stateSize == that.stateSize
                && logicalFileId.equals(that.logicalFileId)
                && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
        int result = getFilePath().hashCode();
        result = 31 * result + Objects.hashCode(startPos);
        result = 31 * result + Objects.hashCode(stateSize);
        result = 31 * result + Objects.hashCode(logicalFileId);
        result = 31 * result + Objects.hashCode(scope);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "Segment File State: %s [Starting Position: %d, %d bytes]",
                getFilePath(), startPos, stateSize);
    }
}
