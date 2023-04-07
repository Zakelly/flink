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

import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.DirectoryStreamStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** A {@link OperatorStreamStateHandle} that works for segmented checkpoints. */
public class SegmentOperatorStreamStateHandle extends OperatorStreamStateHandle
        implements CompositeStateHandle {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(SegmentOperatorStreamStateHandle.class);
    private final DirectoryStreamStateHandle directoryStateHandle;
    private transient SharedStateRegistry sharedStateRegistry;

    public SegmentOperatorStreamStateHandle(
            org.apache.flink.core.fs.Path directoryPath,
            Map<String, StateMetaInfo> stateNameToPartitionOffsets,
            StreamStateHandle segmentFileStateHandle) {
        this(directoryPath.getJavaPath(), stateNameToPartitionOffsets, segmentFileStateHandle);
    }

    public SegmentOperatorStreamStateHandle(
            Path directoryPath,
            Map<String, StateMetaInfo> stateNameToPartitionOffsets,
            StreamStateHandle segmentFileStateHandle) {
        super(stateNameToPartitionOffsets, segmentFileStateHandle);
        directoryStateHandle = new DirectoryStreamStateHandle(directoryPath);
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointId) {
        Preconditions.checkState(
                sharedStateRegistry != stateRegistry,
                "The state handle has already registered its shared states to the given registry.");

        sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

        LOG.trace(
                "Registering SegmentOperatorStreamStateHandle for checkpoint {} from backend.",
                checkpointId);

        stateRegistry.registerReference(
                createStateRegistryKey(directoryStateHandle), directoryStateHandle, checkpointId);
    }

    public static SharedStateRegistryKey createStateRegistryKey(
            DirectoryStateHandle dbSnapshotDirectoryHandle) {
        return new SharedStateRegistryKey(
                dbSnapshotDirectoryHandle.getDirectory().toUri().toString());
    }

    @Override
    public void discardState() throws Exception {
        SharedStateRegistry registry = this.sharedStateRegistry;
        final boolean isRegistered = (registry != null);

        LOG.trace(
                "Discarding SegmentOperatorStreamStateHandle (registered = {}) from backend.",
                isRegistered);

        try {
            getDelegateStateHandle().discardState();
        } catch (Exception e) {
            LOG.warn("Could not properly discard directory state handle.", e);
        }
    }

    @Override
    public long getCheckpointedSize() {
        return getDelegateStateHandle().getStateSize();
    }

    public DirectoryStreamStateHandle getDirectoryStateHandle() {
        return directoryStateHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SegmentOperatorStreamStateHandle that = (SegmentOperatorStreamStateHandle) o;

        return super.equals(that) && directoryStateHandle.equals(that.directoryStateHandle);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(directoryStateHandle);
        return result;
    }

    @Override
    public String toString() {
        return "SegmentOperatorStreamStateHandle{"
                + super.toString()
                + ", directoryStateHandle="
                + directoryStateHandle
                + '}';
    }
}
