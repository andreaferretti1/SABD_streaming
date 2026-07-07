package it.sabd.queries.query1.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Q1JsonSerializationSchema implements KafkaRecordSerializationSchema<Q1ResultWithTime> {

    private final String topic;
    private transient ObjectMapper mapper;

    public Q1JsonSerializationSchema(String topic) {
        this.topic = topic;
    }


    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
        this.mapper = new ObjectMapper();
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Q1ResultWithTime result, KafkaSinkContext kafkaSinkContext, Long aLong) {

        try{
            Map<String, Object> record = new HashMap<>();

            record.put("window_start", result.startWindow);
            record.put("window_end", result.endWindow);
            record.put("airline", result.airline);
            record.put("num_flights", result.totalFlights);
            record.put("completed", result.completed);
            record.put("cancelled", result.cancelled);
            record.put("diverted", result.diverted);
            record.put("dep_delay_mean", result.depDelayMean);
            record.put("cancellation_rate", result.cancellationRate);
            record.put("late_departure_rate", result.depDelayRate);

            byte[] recordByte = this.mapper.writeValueAsBytes(record);

            return new ProducerRecord<>(this.topic, null, recordByte);

        } catch (Exception e){
            throw new RuntimeException("Errore serializzazione JSON per Query 1", e);

        }

    }
}
