package com.jsingh.maven;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class Reading {
    public long timestamp;

    public double voltage_v;
    public double current_a;
    public double power_kw;
    public double reactive_power_kvar;
    public double power_factor;

    public double solar_kw;
    public double wind_kw;

    public double grid_in_kw;
    public double grid_out_kw;

    @JsonProperty("voltage_fluct_%")
    public double voltage_fluct_percent;

    public String fault_indicator;
    public int fault_num;

    public double temperature_c;

    @JsonProperty("humidity_%")
    public double humidity_percent;

    public double electricity_price_gbp_per_kwh;


    @Override
    public String toString() {
        return "timestamp=" + timestamp +
                ", voltage_v=" + voltage_v +
                ", current_a=" + current_a +
                ", power_kw=" + power_kw +
                ", solar_kw=" + solar_kw +
                ", wind_kw=" + wind_kw +
                ", grid_in_kw=" + grid_in_kw +
                ", grid_out_kw=" + grid_out_kw +
                ", fault_indicator=" + fault_indicator;
    }
}
