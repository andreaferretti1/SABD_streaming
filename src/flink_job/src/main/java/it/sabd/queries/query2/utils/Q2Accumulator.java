package it.sabd.queries.query2.utils;

import it.sabd.FlightEvent;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Comparator;
import java.util.PriorityQueue;

public class Q2Accumulator {

    public long ingestionTime = Long.MAX_VALUE;
    public int notCancDivFlights = 0;
    public int totSignificantDelay = 0;
    public Double totDepDelay = null;
    public int notCancDivDelayedFlights = 0;
    public Double maxDepDelay = null;
    public PriorityQueue<Tuple3<String, Integer, Double>> significantDelayedFlights = new PriorityQueue<Tuple3<String, Integer, Double>>(20, Comparator.comparingDouble(flight -> flight.f2));


    public Q2Accumulator(){}


    public void addFlight(FlightEvent event) {

        this.ingestionTime = Math.min(this.ingestionTime, event.ingestionTime);

        Double depDelay = event.DEP_DELAY;

        this.notCancDivFlights++;

        if (depDelay != null) {

            this.notCancDivDelayedFlights++;

            if(this.totDepDelay == null){
                this.totDepDelay = depDelay;
            } else {
                this.totDepDelay += depDelay;
            }
            if (this.maxDepDelay == null) {
                this.maxDepDelay = depDelay;
            } else {
                this.maxDepDelay = Math.max(this.maxDepDelay, depDelay);
            }

            if (depDelay > 30) {
                this.totSignificantDelay++;

                Tuple3<String, Integer, Double> flightData = new Tuple3<>(event.OP_UNIQUE_CARRIER, event.DEST_AIRPORT_ID, event.DEP_DELAY);
                this.addFlightToQueue(flightData);

            }
        }
    }


    public void mergeAccumulator(Q2Accumulator acc){

        this.ingestionTime = Math.min(this.ingestionTime, acc.ingestionTime);
        this.notCancDivFlights = this.notCancDivFlights + acc.notCancDivFlights;
        this.totSignificantDelay = this.totSignificantDelay + acc.totSignificantDelay;

        if(acc.totDepDelay != null) {

            if(this.totDepDelay == null){
                this.totDepDelay = acc.totDepDelay;
            } else {
                this.totDepDelay = this.totDepDelay + acc.totDepDelay;
            }
        }

        this.notCancDivDelayedFlights = this.notCancDivDelayedFlights + acc.notCancDivDelayedFlights;
        if(acc.maxDepDelay != null){
            if(this.maxDepDelay == null){
                this.maxDepDelay = acc.maxDepDelay;
            } else {
                this.maxDepDelay = Math.max(this.maxDepDelay, acc.maxDepDelay);
            }
        }

        for (Tuple3<String, Integer, Double> flight : acc.significantDelayedFlights) {
            this.addFlightToQueue(flight);
        }

    }

    private void addFlightToQueue(Tuple3<String, Integer, Double> flightData){

            if (significantDelayedFlights.size() < 20) {
                this.significantDelayedFlights.offer(flightData);
            } else {
                Tuple3<String, Integer, Double> min = this.significantDelayedFlights.peek();

                if (flightData.f2 > min.f2) {
                    this.significantDelayedFlights.poll();
                    this.significantDelayedFlights.offer(flightData);
                }
            }
    }

}
