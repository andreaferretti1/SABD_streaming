package it.sabd.queries.query2.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q2JsonSerializationSchema implements KafkaRecordSerializationSchema<List<Q2Output>> {

    private final String topic;
    private transient ObjectMapper mapper;

    public Q2JsonSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
        this.mapper = new ObjectMapper();
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(List<Q2Output> top10, KafkaSinkContext kafkaSinkContext, Long aLong) {

        try {
            List<Map<String, Object>> rankRecord = new ArrayList<>();

            for (int i = 0; i < top10.size(); i++) {

                Q2Output airport = top10.get(i);
                int rank = i + 1;

                rankRecord.add(this.getFlightRecord(airport, rank));

            }

            byte[] recordBytes = this.mapper.writeValueAsBytes(rankRecord);

            return new ProducerRecord<>(this.topic, null, recordBytes);

        } catch(Exception e){
            throw new RuntimeException("Errore serializzazione JSON per Query 2", e);
        }
    }

    private Map<String, Object> getFlightRecord(Q2Output airport, int rank){

        Map<String, Object> flightRecord = new HashMap<>();

        flightRecord.put("ts", airport.startWindow);
        flightRecord.put("rank", rank);
        flightRecord.put("origin_airport_id", airport.originAirport);
        flightRecord.put("num_flights", airport.notCancDivFlights);
        flightRecord.put("severe_delays", airport.totSignificantDelayedFLights);
        flightRecord.put("dep_delay_mean", airport.depDelayMean);
        flightRecord.put("dep_delay_max", airport.maxDepDelay);
        flightRecord.put("delayed_flights", this.getDelayedFlightsRecord(airport.top20significantDelayedFLights));

        return flightRecord;
    }


    private List<Map<String, Object>> getDelayedFlightsRecord(List<Tuple3<String, Integer, Double>> top20significantDelayedFLights) {
        List<Map<String, Object>> delayedFlightsRecord = new ArrayList<Map<String, Object>>();
        for(Tuple3<String, Integer, Double> flight: top20significantDelayedFLights){

            Map<String, Object> flightRecord = new HashMap<>();
            flightRecord.put("carrier", flight.f0);
            flightRecord.put("dest_airport", flight.f1);
            flightRecord.put("depDelay", flight.f2);

            delayedFlightsRecord.add(flightRecord);
        }
        return delayedFlightsRecord;
    }
}
