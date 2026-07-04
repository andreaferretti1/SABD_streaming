package it.sabd.queries.query1.utils;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Q1ProcessData extends ProcessWindowFunction<Q1Result, Q1ResultWithTime, String, TimeWindow> {



    @Override
    public void process(String airline,
                        ProcessWindowFunction<Q1Result, Q1ResultWithTime, String, TimeWindow>.Context context,
                        Iterable<Q1Result> results,
                        Collector<Q1ResultWithTime> collector) {

        Q1Result result = results.iterator().next();

        long startWindow = context.window().getStart();
        long endWindow = context.window().getEnd();

        Q1ResultWithTime resultWithTime = new Q1ResultWithTime(startWindow, endWindow, airline, result);

        collector.collect(resultWithTime);
    }
}
