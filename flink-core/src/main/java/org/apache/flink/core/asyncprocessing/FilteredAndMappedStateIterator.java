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

package org.apache.flink.core.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * A {@link StateIterator} which filters and maps the input elements from another {@link
 * StateIterator}.
 *
 * <p>For each element in the input iterator, if it matches the given matcher function, then it will
 * be mapped by the given mapper function and passed to the next iterating function.
 *
 * @param <I> type of input elements
 * @param <O> type of output elements
 */
@Internal
public class FilteredAndMappedStateIterator<I, O> implements StateIterator<O> {

    StateIterator<I> inputIterator;
    Function<I, O> mapper;
    Function<I, Boolean> matcher;

    public FilteredAndMappedStateIterator(
            StateIterator<I> inputIterator, Function<I, Boolean> matcher, Function<I, O> mapper) {
        this.inputIterator = inputIterator;
        this.mapper = mapper;
        this.matcher = matcher;
    }

    @Override
    public <U> StateFuture<Collection<U>> onNext(
            FunctionWithException<O, StateFuture<? extends U>, Exception> iterating) {
        return inputIterator
                .onNext(
                        value -> {
                            return new CompletedAsyncFuture<>(value);
                        })
                .thenCompose(
                        collection -> {
                            Collection<StateFuture<? extends U>> resultFutures = new ArrayList<>();
                            for (I input : collection) {
                                if (matcher == null || matcher.apply(input)) {
                                    O output = mapper.apply(input);
                                    resultFutures.add(iterating.apply(output));
                                }
                            }
                            if (resultFutures.isEmpty()) {
                                return StateFutureUtils.completedFuture(Collections.emptyList());
                            } else {
                                return StateFutureUtils.combineAll(resultFutures);
                            }
                        });
    }

    @Override
    public StateFuture<Void> onNext(ThrowingConsumer<O, Exception> iterating) {
        return inputIterator.onNext(
                input -> {
                    if (matcher == null || matcher.apply(input)) {
                        O output = mapper.apply(input);
                        iterating.accept(output);
                    }
                });
    }

    @Override
    public boolean isEmpty() {
        return inputIterator.isEmpty();
    }
}
