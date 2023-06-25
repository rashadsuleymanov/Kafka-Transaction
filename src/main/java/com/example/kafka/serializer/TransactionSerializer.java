package com.example.kafka.serializer;

import com.example.kafka.payload.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;

@Slf4j
public class TransactionSerializer implements Serializer<Transaction> {

    @Override
    public byte[] serialize(String topic, Transaction transaction) {
        ObjectMapper objectMapper = new ObjectMapper();

        byte[] value = null;
        try {
            value = objectMapper.writeValueAsBytes(transaction);
        } catch (JsonProcessingException e) {
            log.error("Exception during serialization {}", e.getMessage());
        }
        return value;
    }
}
