package it.sabd.queries.query2;

import it.sabd.FlightEvent;
import it.sabd.queries.query2.utils.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;



import java.time.Duration;
import java.util.List;

public class Query2 {

    public static void executeQuery(DataStream<FlightEvent> dataStream){


        KeyedStream<FlightEvent, Integer> keyedStream = dataStream
                .filter(event -> event.CANCELLED == 0.0 && event.DIVERTED == 0.0)
                .keyBy(event -> event.ORIGIN_AIRPORT_ID);


        //Calcolo statistiche finestra di 1 ora
        KafkaSink<List<Q2Output>> kafkaSink1H = KafkaSink.<List<Q2Output>>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(new Q2JsonSerializationSchema("q2_results_1h"))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        keyedStream.window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new Q2AggregateData(), new Q2ProcessData())
                .filter(airport -> airport.notCancDivFlights >= 30)
                .windowAll(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new AggregateTop10())
                .sinkTo(kafkaSink1H)
                .setParallelism(1);


        //Calcolo statistiche finestra di 6 ore
        KafkaSink<List<Q2Output>> kafkaSink6H = KafkaSink.<List<Q2Output>>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(new Q2JsonSerializationSchema("q2_results_6h"))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        keyedStream.window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new Q2AggregateData(), new Q2ProcessData())
                .filter(airport -> airport.notCancDivFlights >= 30)
                .windowAll(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new AggregateTop10())
                .sinkTo(kafkaSink6H)
                .setParallelism(1);


        //Calcolo statistiche finestra globale
        KafkaSink<List<Q2Output>> kafkaSinkGlobal = KafkaSink.<List<Q2Output>>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(new Q2JsonSerializationSchema("q2_results_global"))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        keyedStream.window(GlobalWindows.create())
                .trigger(new EOSTrigger())
                .aggregate(new Q2AggregateData(), new Q2ProcessGlobalData())
                .filter(airport -> airport.notCancDivFlights >= 30)
                .windowAll(GlobalWindows.create())
                .trigger(new EOSTrigger())
                .aggregate(new AggregateTop10())
                .sinkTo(kafkaSinkGlobal)
                .setParallelism(1);


    }

}
