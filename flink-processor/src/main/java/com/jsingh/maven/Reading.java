package com.jsingh.maven;

public class Reading {

    public String city;
    public double temperature;

//    public Reading(String city, double temperature) {
//        this.city = city;
//        this.temperature = temperature;
//    }

    @Override
    public String toString() {
        return "city: " +  city + ", temperature: " + temperature;
    }

}
