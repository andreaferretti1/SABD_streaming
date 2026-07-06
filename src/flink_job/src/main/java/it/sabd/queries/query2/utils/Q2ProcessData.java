package it.sabd.queries.query2.utils;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Q2ProcessData extends ProcessWindowFunction<Q2Result, Q2Output, Integer, TimeWindow> {


    @Override
    public void process(Integer originAirport,
                        ProcessWindowFunction<Q2Result, Q2Output, Integer, TimeWindow>.Context context,
                        Iterable<Q2Result> results,
                        Collector<Q2Output> collector){


            Q2Result result = results.iterator().next();

            long startWindow = context.window().getStart();

            Q2Output output = new Q2Output(
                    startWindow,
                    originAirport,
                    result.notCancDivFlights,
                    result.totSignificantDelayedFLights,
                    result.depDelayMean,
                    result.maxDepDelay,
                    result.top20significantDelayedFlights
            );

            collector.collect(output);
    }
}
