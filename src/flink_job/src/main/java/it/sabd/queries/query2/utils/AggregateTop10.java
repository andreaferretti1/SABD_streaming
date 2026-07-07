package it.sabd.queries.query2.utils;

import org.apache.flink.api.common.functions.AggregateFunction;


public class AggregateTop10 implements AggregateFunction<Q2Output, Top10Accumulator, Top10Output> {

    @Override
    public Top10Accumulator createAccumulator() {
        return new Top10Accumulator();
    }

    @Override
    public Top10Accumulator add(Q2Output output, Top10Accumulator acc) {

        acc.addEvent(output);

        return acc;
    }

    @Override
    public Top10Output getResult(Top10Accumulator acc) {
        return Top10Output.getTop10Output(acc);
    }

    @Override
    public Top10Accumulator merge(Top10Accumulator acc1, Top10Accumulator acc2) {

        acc1.merge(acc2);



        return acc1;
    }
}
