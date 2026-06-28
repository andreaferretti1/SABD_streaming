package it.sabd;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public class JSONDeserializationSchema implements DeserializationSchema {

    private ObjectMapper mapper;

    
}
