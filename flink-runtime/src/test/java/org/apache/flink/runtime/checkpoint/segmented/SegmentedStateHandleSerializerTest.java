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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/** Tests for serialization of segmented state handles. */
public class SegmentedStateHandleSerializerTest {

    private static final Random rnd = new Random();

    @Test
    public void testSerializeSegmentFileStateHandle() throws IOException {
        SegmentFileStateHandle handle = getRandomSegmentFileStateHandle();
        verify(
                handle,
                MetadataV3Serializer::serializeStreamStateHandle,
                MetadataV3Serializer::deserializeStreamStateHandle);
    }

    @Test
    public void testSerializeSegmentOperatorStreamStateHandle() throws IOException {
        SegmentOperatorStreamStateHandle handle = getRandomSegmentOperatorStreamStateHandle();
        verify(
                handle,
                MetadataV3Serializer::serializeOperatorStateHandleUtil,
                MetadataV3Serializer::deserializeOperatorStateHandleUtil);
        SegmentOperatorStreamStateHandle empty =
                SegmentCheckpointUtils.emptyOperatorStreamStateHandle(new Path("test-managed-dir"));
        verify(
                empty,
                MetadataV3Serializer::serializeOperatorStateHandleUtil,
                MetadataV3Serializer::deserializeOperatorStateHandleUtil);
    }

    private SegmentFileStateHandle getRandomSegmentFileStateHandle() {
        return new SegmentFileStateHandle(
                new Path(String.valueOf(UUID.randomUUID())),
                rnd.nextInt(100),
                rnd.nextInt(1000) + 100,
                new LogicalFile.LogicalFileId(String.valueOf(UUID.randomUUID())),
                CheckpointedStateScope.EXCLUSIVE);
    }

    private SegmentOperatorStreamStateHandle getRandomSegmentOperatorStreamStateHandle() {
        Map<String, OperatorStateHandle.StateMetaInfo> namedStatesToOffsets = new HashMap<>();
        int numNamedStates = 2;
        int maxPartitionsPerState = 128;
        Random r = new Random();
        int off = 0;
        for (int s = 0; s < numNamedStates - 1; ++s) {
            long[] offs = new long[1 + r.nextInt(maxPartitionsPerState)];

            for (int o = 0; o < offs.length; ++o) {
                offs[o] = off;
                ++off;
            }

            OperatorStateHandle.Mode mode =
                    r.nextInt(10) == 0
                            ? OperatorStateHandle.Mode.UNION
                            : OperatorStateHandle.Mode.SPLIT_DISTRIBUTE;
            namedStatesToOffsets.put(
                    "State-" + s, new OperatorStateHandle.StateMetaInfo(offs, mode));
        }
        SegmentFileStateHandle stateHandle = getRandomSegmentFileStateHandle();
        return new SegmentOperatorStreamStateHandle(
                new Path("test-dir"), namedStatesToOffsets, stateHandle);
    }

    private <T extends StateObject> void verify(
            T handle,
            BiConsumerWithException<T, DataOutputStream, IOException> serializer,
            FunctionWithException<DataInputStream, T, IOException> deserializer)
            throws IOException {
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            serializer.accept(handle, new DataOutputStream(out));
            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                StateObject deserialized = deserializer.apply(new DataInputStream(in));
                assertEquals(handle, deserialized);
            }
        }
    }
}
