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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The list version of TtlAwareSerializer.
 * @param <T>
 */
public class TtlAwareListSerializer<T> extends TtlAwareSerializer<List<T>, ListSerializer<T>> {

    public TtlAwareListSerializer(ListSerializer<T> typeSerializer) {
        super(typeSerializer);
    }

    // ------------------------------------------------------------------------
    //  ListSerializer specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the serializer for the elements of the list.
     *
     * @return The serializer for the elements of the list
     */
    public TypeSerializer<T> getElementSerializer() {
        return getOriginalTypeSerializer().getElementSerializer();
    }
}
