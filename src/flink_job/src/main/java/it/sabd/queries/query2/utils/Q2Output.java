package it.sabd.queries.query2.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

public class Q2Output {

    public long minIngestionTime;
    public long startWindow;
    public int originAirport;
    public int notCancDivFlights;
    public int totSignificantDelayedFLights;
    public Double depDelayMean;
    public Double maxDepDelay;
    public List<Tuple3<String, Integer, Double>> top20significantDelayedFLights;


    public Q2Output(){}


    public Q2Output(long minIngestionTime,
                    long startWindow,
                    int originAirport,
                    int totalFlights,
                    int totSignificantDelayedFLights,
                    Double depDelayMean,
                    Double maxDepDelay,
                    List<Tuple3<String, Integer, Double>> top20significantDelayedFLights) {
        this.minIngestionTime = minIngestionTime;
        this.startWindow = startWindow;
        this.originAirport = originAirport;
        this.notCancDivFlights = totalFlights;
        this.totSignificantDelayedFLights = totSignificantDelayedFLights;
        this.depDelayMean = depDelayMean;
        this.maxDepDelay = maxDepDelay;
        this.top20significantDelayedFLights = top20significantDelayedFLights;
    }
}
