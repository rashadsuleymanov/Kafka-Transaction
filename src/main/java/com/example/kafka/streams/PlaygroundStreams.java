package com.example.kafka.streams;

import lombok.extern.slf4j.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

import static com.example.kafka.common.Util.*;

@Slf4j
public class PlaygroundStreams {

    public static void main(String[] args) {
        String prefix = "B";
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-pattern");


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //source
        KStream<String, String> sourceStream1 = streamsBuilder.stream(SOURCE_TEXT_TOPIC1);
        KStream<String, String> sourceStream2 = streamsBuilder.stream(SOURCE_TEXT_TOPIC2);

//        //split -words processor
//        KStream<String, String> wordsProcessor = sourceStream.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
//
//        //matching pattern processor
//        KStream<String, String> matchedProcessor = wordsProcessor.filter((key, value) -> value.toUpperCase().startsWith(prefix));
//
//        matchedProcessor.to(WORD_TOPIC);

        sourceStream1.merge(sourceStream2)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .filter((key, value) -> value.toUpperCase().startsWith(prefix))
                .filterNot((key, value) -> value.toUpperCase().endsWith(prefix))
                .peek((key, value) -> System.out.println(value))
                // .foreach((key, value) -> System.out.println(value))
                .map((key, value) -> new KeyValue<>(key.toUpperCase(), value.toUpperCase()))
                .mapValues(value -> value.concat("_NEW"))
                .selectKey((key, value) -> value.length())
                .print(Printed.toSysOut());
//                .to(new TopicNameExtractor<Integer, String>() {
//                    @Override
//                    public String extract(Integer key, String value, RecordContext recordContext) {
//                        return recordContext.topic().concat("-1");
//                    }
//                });

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
