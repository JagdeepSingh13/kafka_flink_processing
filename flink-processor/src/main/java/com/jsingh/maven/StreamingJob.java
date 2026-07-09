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

package com.jsingh.maven;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableClosureCleaner();

		KafkaSource<Reading> source = KafkaSource.<Reading>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("smartgrid")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new ReadingDeserialization())
				.build();

//		to use the timestamp attribute for dividing data needed for windows
//		withIdleness is needed to ignore WM from partiton which has not received any events
		WatermarkStrategy<Reading> watermarkStrategy = WatermarkStrategy
				.<Reading>forMonotonousTimestamps()
				.withTimestampAssigner((event, timestamp) -> event.timestamp * 1000)
				.withIdleness(Duration.ofSeconds(5));

		DataStreamSource<Reading> readingStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

//		[i] filtering data
//		DataStream<Reading> unhealthyGridEvents = readingStream.
//				filter(StreamingJob::filterOutHealthyGridEvents);

//		[ii] counting events
//		we give 1 to all fault types and keyBy f0 is used to group by 1st arg of Tuple
/*
		DataStream<Tuple2<String, Integer>> counts = readingStream
				.map(StreamingJob::countByFault)
				.keyBy(t -> t.f0)
				.sum(1);
*/

//		[iii] windows
/*
		DataStream<Tuple2<String, Integer>> windowCounts = readingStream
				.map(StreamingJob::countByFault)
				.keyBy(t -> t.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.sum(1);
*/

//		[iv] half-hourly average values -> of 1 attribute
//		inc/dec window size depending on generation rate
/*
		DataStream<Double> avgWind = readingStream
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
				.aggregate(new WindAccumulator())
				;
*/

//		[v] half-hourly averages of multiple attributes
//		part 2 of this is ProcessWindowFunction -> gives and lets us use context of window
		DataStream<HalfHourAverages> averages = readingStream
				.windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
				.aggregate(new Accumulator(), new MyProcessWindowFunction())
				;

//		readingStream.print();
//		unhealthyGridEvents.print();
//		counts.print();
//		windowCounts.print();
//		avgWind.print();
		averages.print();

		env.execute("testing flink");
	}

	private static boolean filterOutHealthyGridEvents(Reading r) {
		return !r.fault_indicator.equals("normal");
	}

	public static Tuple2<String, Integer> countByFault(Reading r) {
		return Tuple2.of(r.fault_indicator, 1);
	}

}
