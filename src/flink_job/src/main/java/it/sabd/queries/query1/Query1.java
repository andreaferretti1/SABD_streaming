package it.sabd.queries.query1;

import it.sabd.FlightEvent;
import it.sabd.queries.query1.utils.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class Query1 {

    public static void executeQuery(DataStream<FlightEvent> dataStream){

        KafkaSink<Q1ResultWithTime> kafkaSink = KafkaSink.<Q1ResultWithTime>builder()
                        .setBootstrapServers("kafka:9092")
                        .setRecordSerializer(new Q1JsonSerializationSchema("q1-results"))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        dataStream.filter(event -> event.OP_UNIQUE_CARRIER.equals("AA") ||
                event.OP_UNIQUE_CARRIER.equals("DL") ||
                event.OP_UNIQUE_CARRIER.equals("UA") ||
                event.OP_UNIQUE_CARRIER.equals("WN")
                )
                .keyBy(event -> event.OP_UNIQUE_CARRIER)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new Q1AggregateData(), new Q1ProcessData())
                .process(new GaugeFunction())
                .sinkTo(kafkaSink);
    }


}
