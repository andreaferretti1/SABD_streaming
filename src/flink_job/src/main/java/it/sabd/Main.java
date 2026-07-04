package it.sabd;


import it.sabd.queries.query1.Query1;
import it.sabd.queries.query2.Query2;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Main {

    public static void main(String[] args) throws Exception {

        KafkaSource<FlightEvent> kafkaSource = KafkaSource.<FlightEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("data")
                .setValueOnlyDeserializer(new JSONDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStream<FlightEvent> stream = env.fromSource(kafkaSource,
                WatermarkStrategy.<FlightEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> timestamp),
                "kafkaSource");

        Query1.executeQuery(stream);
        Query2.executeQuery(stream);




        env.execute("flight_job");

    }

}
