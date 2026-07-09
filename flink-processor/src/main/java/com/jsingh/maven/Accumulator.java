package com.jsingh.maven;

import org.apache.flink.api.common.functions.AggregateFunction;

public class Accumulator
        implements AggregateFunction<Reading, HalfHourAccumulator, HalfHourAccumulator> {

    @Override
    public HalfHourAccumulator createAccumulator() {
        return new HalfHourAccumulator();
    }

    @Override
    public HalfHourAccumulator add(Reading reading, HalfHourAccumulator halfHourAccumulator) {
        halfHourAccumulator.sumOfWind += reading.wind_kw;
        halfHourAccumulator.sumOfSolar += reading.solar_kw;
        halfHourAccumulator.count += 1L;

        return halfHourAccumulator;
    }

    @Override
    public HalfHourAccumulator getResult(HalfHourAccumulator halfHourAccumulator) {
        /*
        HalfHourAverages result = new HalfHourAverages();
        result.avgWind = halfHourAccumulator.sumOfWind / halfHourAccumulator.count;
        result.avgSolar = halfHourAccumulator.sumOfSolar / halfHourAccumulator.count;

        return result;
         */

//        as we added the process that takes in I/P that is O/P of accumulator
        return halfHourAccumulator;
    }

    @Override
    public HalfHourAccumulator merge(HalfHourAccumulator a, HalfHourAccumulator b) {
        a.sumOfWind += b.sumOfWind;
        a.sumOfSolar += b.sumOfSolar;
        a.count += b.count;
        return a;
    }
}
