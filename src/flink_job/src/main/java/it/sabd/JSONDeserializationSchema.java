package it.sabd;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONDeserializationSchema implements DeserializationSchema<FlightEvent> {

    private transient ObjectMapper mapper;


    @Override
    public void open(InitializationContext context) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    public FlightEvent deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, FlightEvent.class);
    }

    @Override
    public boolean isEndOfStream(FlightEvent flightEvent) {
        return false;
    }

    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }
}
