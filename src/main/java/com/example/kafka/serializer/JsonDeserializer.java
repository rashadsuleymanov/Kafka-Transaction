package com.example.kafka.serializer;

import com.example.kafka.payload.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {

    private Class<T> className;

    public JsonDeserializer(Class<T> className) {
        this.className = className;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {

        if (bytes == null) return null;

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.readValue(bytes, className);
        } catch (IOException e) {
            log.error("Exception during JSON desiralizer {}", e.getMessage());
            throw new SerializationException(e);
        }
    }
}
