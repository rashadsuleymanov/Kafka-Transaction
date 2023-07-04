package com.example.kafka.streams;

import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

import static com.example.kafka.common.Util.*;

@Slf4j
public class KafkaStreamsExample {
    public static void main(String[] args) {

        String prefix = "B";
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-pattern");


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //source
        KStream<String, String> sourceStream = streamsBuilder.stream(SOURCE_TEXT_TOPIC1);

        //split -words processor
        KStream<String, String> wordsProcessor = sourceStream.flatMapValues(value -> Arrays.asList(value.split("\\W+")));

        //matching pattern processor
        KStream<String, String> matchedProcessor = wordsProcessor.filter((key, value) -> value.toUpperCase().startsWith(prefix));

        matchedProcessor.to(WORD_TOPIC);

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
