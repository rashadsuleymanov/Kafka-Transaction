package com.example.kafka.producer;

import com.example.kafka.partitioner.TransactionPartitioner;
import com.example.kafka.payload.*;
import com.example.kafka.serializer.TransactionSerializer;
import lombok.extern.slf4j.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.math.*;
import java.util.*;

import static com.example.kafka.common.Util.*;

@Slf4j
public class ProducerAPI {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName()); // Transaction-a aid Serializer olacaq bu hissede
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TransactionPartitioner.class.getName());

        KafkaProducer<Integer, Transaction> producer = new KafkaProducer<>(properties);


        for (int i = 0; i <= 30; i++) {
            Transaction transaction = new Transaction(i, UUID.randomUUID().toString(), new BigDecimal(i * 10));
            ProducerRecord<Integer, Transaction> record = new ProducerRecord<>(TOPIC_NAME, i, transaction);
            producer.send(record);

            log.info("Data was sent: {}", transaction.toString());
        }

        producer.close();


    }
}
