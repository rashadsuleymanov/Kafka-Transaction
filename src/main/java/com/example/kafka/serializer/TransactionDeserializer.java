package com.example.kafka.serializer;

import com.example.kafka.payload.*;
import com.fasterxml.jackson.databind.*;
import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;

import java.io.*;

@Slf4j
public class TransactionDeserializer implements Deserializer<Transaction> {

    @Override
    public Transaction deserialize(String topic, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();

        Transaction transaction = null;
        try {
            transaction = objectMapper.readValue(bytes, Transaction.class);
        } catch (IOException e) {
            log.error("Exception during desiralizer {}", e.getMessage());
        }
        return transaction;
    }
}
