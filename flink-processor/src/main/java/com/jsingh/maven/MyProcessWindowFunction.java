package com.jsingh.maven;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class MyProcessWindowFunction
    extends ProcessAllWindowFunction<HalfHourAccumulator, HalfHourAverages, TimeWindow> {

    @Override
//    process(window, input accumulator, output collector)
    public void process(
            Context context,
            Iterable<HalfHourAccumulator> accumulatorIt,
            Collector<HalfHourAverages> out
    ) throws Exception {
        HalfHourAverages averages = new HalfHourAverages();
        HalfHourAccumulator accumulator = accumulatorIt.iterator().next();

        averages.avgSolar = accumulator.sumOfSolar / accumulator.count;
        averages.avgWind = accumulator.sumOfWind / accumulator.count;
        averages.windowStart = Instant.ofEpochMilli(context.window().getStart());

        out.collect(averages);
    }

}
