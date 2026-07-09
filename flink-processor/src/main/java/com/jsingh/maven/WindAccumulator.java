package com.jsingh.maven;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

// AggregateFunction takes in 3 args -> input, accumulator, output

public class WindAccumulator
        implements AggregateFunction<Reading, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
//        {sum of wind, count}
        return new Tuple2<>(0.0, 0L);
    }

    @Override
//    add(input, accumulator) -> accumulator
    public Tuple2<Double, Long> add(Reading reading, Tuple2<Double, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + reading.wind_kw, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return (accumulator.f0 / accumulator.f1);
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }

}
