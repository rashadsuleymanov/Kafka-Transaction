package com.example.kafka.serde;

import com.example.kafka.payload.*;
import com.example.kafka.serializer.*;
import org.apache.kafka.common.serialization.*;

public class CustomSerdes {

    public static Serde<Transaction> Transaction() {
        return new TransactionSerde();
    }

    public static Serde<Transaction> TransactionJson() {
        return new TransactionJsonSerde();
    }

    public static final class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
        public TransactionSerde() {
            super(new TransactionSerializer(), new TransactionDeserializer());
        }
    }

    public static final class TransactionJsonSerde extends Serdes.WrapperSerde<Transaction> {
        public TransactionJsonSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Transaction.class));
        }
    }

}
