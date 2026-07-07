package it.sabd.queries.query2.utils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class GaugeFunction extends ProcessFunction<Top10Output, List<Q2Output>> {

    private transient volatile long latestLatency = 0L;
    private final String queryId;

    public GaugeFunction(String queryId) {
        this.queryId = queryId;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        getRuntimeContext()
                .getMetricGroup()
                .addGroup("query_id", this.queryId)
                .gauge("endToEndLatency", () -> this.latestLatency);
    }


    @Override
    public void processElement(Top10Output top10Output, ProcessFunction<Top10Output, List<Q2Output>>.Context context, Collector<List<Q2Output>> collector) throws Exception {
        this.latestLatency = System.currentTimeMillis() - top10Output.minIngestionTime;

        collector.collect(top10Output.top10);
    }


}
