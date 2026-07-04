package it.sabd.queries.query1.utils;

public class Q1Result {

    public int totalFlights;
    public int completed;
    public int cancelled;
    public int diverted;
    public Double depDelayMean;
    public double cancellationRate;
    public Double depDelayRate;


    public Q1Result(){}

    public Q1Result(Q1Result other) {
        this.totalFlights = other.totalFlights;
        this.completed = other.completed;
        this.cancelled = other.cancelled;
        this.diverted = other.diverted;
        this.depDelayMean = other.depDelayMean;
        this.cancellationRate = other.cancellationRate;
        this.depDelayRate = other.depDelayRate;
    }


    public static Q1Result computeResult(Q1Accumulator acc){

        Q1Result result = new Q1Result();

        result.totalFlights = acc.totalFlights;
        result.completed = acc.completed;
        result.cancelled = acc.cancelled;
        result.diverted = acc.diverted;

        if(acc.validDelayFlights != 0) {
            result.depDelayMean = acc.totalDepDelay / acc.validDelayFlights;
        } else {
            result.depDelayMean = null;
        }


        result.cancellationRate = (double) acc.cancelled / acc.totalFlights;

        int notCancelledFlights = acc.totalFlights - acc.cancelled;
        if(notCancelledFlights != 0) {
            result.depDelayRate = (double) acc.lateFlightsGreater15 / notCancelledFlights;
        } else {
            result.depDelayRate = null;
        }

        return result;

    }
}
