package it.sabd.queries.query1.utils;

import it.sabd.FlightEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class Q1AggregateData implements AggregateFunction<FlightEvent, Q1Accumulator, Q1Result> {


    @Override
    public Q1Accumulator createAccumulator() {
        return new Q1Accumulator();
    }

    @Override
    public Q1Accumulator add(FlightEvent event, Q1Accumulator acc) {

        acc.addFlight(event);
        return acc;
    }

    @Override
    public Q1Result getResult(Q1Accumulator acc) {

        return Q1Result.computeResult(acc);

    }

    @Override
    public Q1Accumulator merge(Q1Accumulator acc1, Q1Accumulator acc2) {

        acc1.mergeAcc(acc2);
        return acc1;

    }
}
