package it.sabd;


import it.sabd.queries.query1.Query1;
import it.sabd.queries.query2.Query2;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Main {

    public static void main(String[] args) throws Exception {

        KafkaSource<FlightEvent> kafkaSource = KafkaSource.<FlightEvent>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("data")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new JSONDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStream<FlightEvent> stream = env.fromSource(kafkaSource,
                WatermarkStrategy.<FlightEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> timestamp),
                "kafkaSource")
                .setParallelism(1);

        Query1.executeQuery(stream);
        Query2.executeQuery(stream);

        env.execute("flight_job");

    }

}
