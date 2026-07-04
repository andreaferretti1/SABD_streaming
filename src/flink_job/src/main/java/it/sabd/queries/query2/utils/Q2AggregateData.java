package it.sabd.queries.query2.utils;

import it.sabd.FlightEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class Q2AggregateData implements AggregateFunction<FlightEvent, Q2Accumulator, Q2Result> {
    @Override
    public Q2Accumulator createAccumulator() {
        return new Q2Accumulator();
    }

    @Override
    public Q2Accumulator add(FlightEvent event, Q2Accumulator acc) {
        acc.addFlight(event);
        return acc;
    }

    @Override
    public Q2Result getResult(Q2Accumulator acc) {
        return Q2Result.computeResult(acc);
    }

    @Override
    public Q2Accumulator merge(Q2Accumulator acc1, Q2Accumulator acc2) {
        acc1.mergeAccumulator(acc2);
        return acc1;
    }
}
