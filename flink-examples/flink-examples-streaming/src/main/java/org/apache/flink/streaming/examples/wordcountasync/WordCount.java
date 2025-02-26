/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.examples.wordcountasync;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.state.forst.ForStOptions.CACHE_SIZE_BASE_LIMIT;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.CHECKPOINT_INTERVAL;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.CHECKPOINT_PATH;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.JOB_NAME;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.STATE_MODE;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.TTL;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_LENGTH;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_NUMBER;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_RATE;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.configureCheckpoint;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.getConfiguration;

/**
 * Benchmark mainly used for {@link ValueState} and only support 1 parallelism.
 */
public class WordCount {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = getConfiguration(params);
        configRemoteStateBackend(configuration);
        configuration.set(CHECKPOINTS_DIRECTORY, "file:///tmp/checkpoints");
        configuration.set(CHECKPOINT_INTERVAL, 30000L);

        configureCheckpoint(env, configuration);

		env.getConfig().setGlobalJobParameters(configuration);
        env.configure(configuration);
		//env.disableOperatorChaining();

		String jobName = configuration.get(JOB_NAME);

		String group1 = "default1";
		String group2 = "default2";
		String group3 = "default3";
        group1 = group2 = group3 = "default";


		// configure source
		int wordNumber = configuration.get(WORD_NUMBER);
		int wordLength = configuration.get(WORD_LENGTH);
		int wordRate = configuration.get(WORD_RATE);


		DataStream<Tuple2<String, Long>> source =
				WordSource.getSource(env, wordRate, wordNumber, wordLength)
                        .slotSharingGroup(group1).setParallelism(1);

		// configure ttl
		long ttl = configuration.get(TTL).toMillis();

		FlatMapFunction<Tuple2<String, Long>, Long> flatMapFunction =
			getFlatMapFunction(configuration, ttl);
		DataStream<Long> mapper = source.keyBy(e -> e.f0)
                .enableAsyncState()
				.flatMap(flatMapFunction)
				.setParallelism(1)
                .slotSharingGroup(group2);

		mapper.addSink(new BlackholeSink<>()).setParallelism(1).slotSharingGroup(group3);
        // mapper.addSink(new BlackholeSink<>()).setParallelism(1).slotSharingGroup(group3);

		if (jobName == null) {
			env.execute();
		} else {
			env.execute(jobName);
		}
	}

    private static void configRemoteStateBackend(Configuration config) {
        config.set(STATE_BACKEND, "forst");
        config.set(CACHE_SIZE_BASE_LIMIT, MemorySize.ofMebiBytes(60));
    }

	private static FlatMapFunction<Tuple2<String, Long>, Long> getFlatMapFunction(Configuration configuration, long ttl) {
		JobConfig.StateMode stateMode =
			JobConfig.StateMode.valueOf(configuration.get(STATE_MODE).toUpperCase());

		switch (stateMode) {
			case MIXED:
			default:
				return new MixedFlatMapper(ttl);
		}
	}

	public static class CustomTypeSerializer extends TypeSerializerSingleton<Object> {

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public Object createInstance() {
			return 0;
		}

		@Override
		public Object copy(Object o) {
			return ((Integer) o).intValue();
		}

		@Override
		public Object copy(Object o, Object t1) {
			return null;
		}

		@Override
		public int getLength() {
			return 4;
		}

		@Override
		public void serialize(Object o, DataOutputView dataOutputView) throws IOException {
			System.out.println("Serializing " + o.toString());
			dataOutputView.writeInt((Integer) o);
		}

		@Override
		public Object deserialize(DataInputView dataInputView) throws IOException {
			int a = dataInputView.readInt();
			System.out.println("Deserializing " + a);
			return a;
		}

		@Override
		public Object deserialize(Object o, DataInputView dataInputView) throws IOException {
			int a = dataInputView.readInt();
			System.out.println("Deserializing " + a);
			return a;
		}

		@Override
		public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
			dataOutputView.write(dataInputView, 4);
		}

		@Override
		public TypeSerializerSnapshot<Object> snapshotConfiguration() {
			return new CustomTypeSerializerSnapshot();
		}
	};

	static CustomTypeSerializer INSTANCE = new CustomTypeSerializer();

	public static final class CustomTypeSerializerSnapshot extends SimpleTypeSerializerSnapshot<Object> {
		public CustomTypeSerializerSnapshot() {
			super(() -> {
				return INSTANCE;
			});
		}
	}

	/**
	 * Write and read mixed mapper.
	 */
	public static class MixedFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

		private transient ValueState<Integer> wordCounter;

		private final long ttl;

        private final AtomicLong processed = new AtomicLong(0L);

		public MixedFlatMapper(long ttl) {
			this.ttl = ttl;
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
            wordCounter.asyncValue().thenAccept(currentValue -> {
                if (currentValue != null) {
                    wordCounter.asyncUpdate(currentValue + 1).thenAccept(empty -> {
                        out.collect(processed.incrementAndGet());
                    });
                } else {
                    wordCounter.asyncUpdate(1).thenAccept(empty -> {
                        out.collect(processed.incrementAndGet());
                    });
                }
            });
		}

		@Override
		public void open(OpenContext ignore) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc",
                            TypeInformation.of(new TypeHint<Integer>(){}));
			if (ttl > 0) {
				LOG.info("Setting ttl to {}ms.", ttl);
				StateTtlConfig ttlConfig = StateTtlConfig
						.newBuilder(Duration.ofMillis(ttl))
						.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
			}
			wordCounter = getRuntimeContext().getState(descriptor);
		}
	}
}
