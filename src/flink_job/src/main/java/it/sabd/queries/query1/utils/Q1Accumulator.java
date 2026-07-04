package it.sabd.queries.query1.utils;

import it.sabd.FlightEvent;

public class Q1Accumulator{

    public int totalFlights = 0;
    public int completed = 0;
    public int cancelled = 0;
    public int diverted = 0;
    public double totalDepDelay = 0;
    public int validDelayFlights = 0;
    public int lateFlightsGreater15 = 0;


    public Q1Accumulator(){}


    public void addFlight(FlightEvent event){

        this.totalFlights++;

        boolean isCancelled = event.CANCELLED == 1.0;
        boolean isDiverted = event.DIVERTED == 1.0;

        if (isCancelled){
            this.cancelled++;
        } else if (isDiverted){
            this.diverted++;
        } else {
            this.completed++;
        }

        if(!isCancelled && event.DEP_DELAY != null){

            this.validDelayFlights++;
            this.totalDepDelay += event.DEP_DELAY;

            if(event.DEP_DELAY > 15) this.lateFlightsGreater15++;

        }
    }


    public void mergeAcc(Q1Accumulator acc){

        this.totalFlights = this.totalFlights + acc.totalFlights;
        this.completed = this.completed + acc.completed;
        this.cancelled = this.cancelled + acc.cancelled;
        this.diverted = this.diverted + acc.diverted;
        this.totalDepDelay = this.totalDepDelay + acc.totalDepDelay;
        this.validDelayFlights = this.validDelayFlights + acc.validDelayFlights;
        this.lateFlightsGreater15 = this.lateFlightsGreater15 + acc.lateFlightsGreater15;



    }

}
