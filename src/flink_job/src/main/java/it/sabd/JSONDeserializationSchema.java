package it.sabd;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JSONDeserializationSchema implements KafkaRecordDeserializationSchema<FlightEvent> {

    private transient ObjectMapper mapper;
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
        this.mapper = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<FlightEvent> collector) throws IOException {
        JsonNode node = this.mapper.readTree(consumerRecord.value());

        if(node.has("eos")) {
          FlightEvent event = FlightEvent.generateDummyFlightEvent();
          collector.collect(event);
          return;
        }

        FlightEvent flightEvent = this.mapper.treeToValue(node, FlightEvent.class);
        collector.collect(flightEvent);
    }

    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }
}
