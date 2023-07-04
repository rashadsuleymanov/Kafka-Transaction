package com.example.kafka.serializer;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;

@Slf4j
public class JsonSerializer <T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        ObjectMapper objectMapper = new ObjectMapper();

        byte[] value = null;
        try {
            value = objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("Exception during JSON serialization {}", e.getMessage());
        }
        return value;
    }
}
