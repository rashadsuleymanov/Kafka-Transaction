package com.example.kafka.consumer;

import com.example.kafka.payload.*;
import com.example.kafka.serializer.TransactionDeserializer;
import lombok.extern.slf4j.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.*;
import java.util.*;

import static com.example.kafka.common.Util.*;

@Slf4j
public class ConsumerAPI {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName()); //
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-cons-group-1");

        KafkaConsumer<Integer, Transaction> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        try {

            while (true) {

                ConsumerRecords<Integer, Transaction> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord record : records) {

                    if (record.value() != null) {
                        Transaction transaction = (Transaction) record.value();
                        String topic = record.topic();
                        int partition = record.partition();
                        log.info("Received data. Transaction {}, topic {}, partition {}", transaction.toString(),
                                topic, partition);

                    }
                }
            }

        } catch (Exception ex) {
            log.error("Exception {}", ex.getMessage());
        } finally {
            consumer.close();
        }

    }
}
