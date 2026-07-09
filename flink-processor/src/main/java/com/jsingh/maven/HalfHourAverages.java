package com.jsingh.maven;

import java.time.Instant;

public class HalfHourAverages {

    public double avgWind;
    public double avgSolar;
    public Instant windowStart;

    @Override
    public String toString() {
        return "avgWind: " + avgWind +
                ", avgSolar: " + avgSolar +
                ", windowStart: " + windowStart;
    }

}
