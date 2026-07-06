package it.sabd.queries.query2.utils;


import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class Q2Result {

    public int notCancDivFlights;
    public int totSignificantDelayedFLights;
    public Double depDelayMean;
    public Double maxDepDelay;
    public List<Tuple3<String, Integer, Double>> top20significantDelayedFlights;


    public Q2Result() {}


    public static Q2Result computeResult(Q2Accumulator acc){

        Q2Result result = new Q2Result();

        result.notCancDivFlights = acc.notCancDivFlights;
        result.totSignificantDelayedFLights = acc.totSignificantDelay;

        if(acc.totDepDelay != null && acc.notCancDivDelayedFlights != 0) {
            result.depDelayMean = acc.totDepDelay / acc.notCancDivDelayedFlights;
        } else {
            result.depDelayMean = null;
        }


        result.maxDepDelay = acc.maxDepDelay;

        PriorityQueue<Tuple3<String, Integer, Double>> queueCopy = new PriorityQueue<>(acc.significantDelayedFlights);
        result.top20significantDelayedFlights = new ArrayList<Tuple3<String, Integer, Double>>();

        while (!queueCopy.isEmpty()) {
            result.top20significantDelayedFlights.add(queueCopy.poll());
        }

        Collections.reverse(result.top20significantDelayedFlights);

        return result;

    }
}
