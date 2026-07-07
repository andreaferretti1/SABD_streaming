package it.sabd.queries.query1.utils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class GaugeFunction extends ProcessFunction<Q1ResultWithTime, Q1ResultWithTime> {

    private transient volatile long latestLatency = 0L;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        String runId = System.getenv("RUN_ID") != null ? System.getenv("RUN_ID") : "0";
        String parallelism = System.getenv("FLINK_PARALLELISM") != null ? System.getenv("FLINK_PARALLELISM") : "1";

        getRuntimeContext()
                .getMetricGroup()
                .addGroup("query_id", "query_1")
                .addGroup("run_id", runId)
                .addGroup("parallelism", parallelism)
                .gauge("endToEndLatency", () -> this.latestLatency);
    }


    @Override
    public void processElement(Q1ResultWithTime q1ResultWithTime, ProcessFunction<Q1ResultWithTime, Q1ResultWithTime>.Context context, Collector<Q1ResultWithTime> collector) throws Exception {
        this.latestLatency = System.currentTimeMillis() - q1ResultWithTime.minIngestionTime;

        collector.collect(q1ResultWithTime);
    }
}
