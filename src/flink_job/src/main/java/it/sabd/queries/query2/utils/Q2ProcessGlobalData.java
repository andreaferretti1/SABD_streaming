package it.sabd.queries.query2.utils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;

public class Q2ProcessGlobalData extends ProcessWindowFunction<Q2Result, Q2Output, Integer, GlobalWindow>{

    public static final long startWindow = LocalDate.of(2025, Month.JANUARY, 1)
            .atStartOfDay()
            .atZone(ZoneId.of("UTC"))
            .toInstant()
            .toEpochMilli();

    @Override
    public void process(Integer originAirport, ProcessWindowFunction<Q2Result, Q2Output, Integer, GlobalWindow>.Context context, Iterable<Q2Result> results, Collector<Q2Output> collector) {

        Q2Result result = results.iterator().next();

        Q2Output output = new Q2Output(
                result.minIngestionTime,
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

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }
}
