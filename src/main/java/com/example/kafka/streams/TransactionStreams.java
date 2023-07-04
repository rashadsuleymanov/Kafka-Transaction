package com.example.kafka.streams;

import com.example.kafka.payload.*;
import com.example.kafka.serde.*;
import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.math.*;
import java.util.*;

import static com.example.kafka.common.Util.*;

@Slf4j
public class TransactionStreams {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.Transaction().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.TransactionJson().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-transaction");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Transaction> sourceStream = streamsBuilder.stream(TOPIC_NAME);

        //amount > 1500

        KStream<String, Transaction> transactionKStream = sourceStream
                .filter((key, value) -> value.getAmount().compareTo(BigDecimal.valueOf(1500)) > 0);

        //send  to the streams-big-tansactions

        transactionKStream.to(TOPIC_BIG_TRANSACTIONS);

        Topology topology = streamsBuilder.build();

        KafkaStreams streams = new KafkaStreams(topology, properties);

        log.info("Starting stream.....");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("closing stream.....");
            streams.close();
        }));

    }
}
