package com.example.kafka.partitioner;

import com.example.kafka.payload.Transaction;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.*;

public class TransactionPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {

        List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic); //butun partition-lar haqqinda melumat almaq istedikde

        int partitionSize = partitionInfoList.size();

        if (partitionSize != 15) {
            throw new InvalidTopicException("Topic daxilinde partition-larin sayi 15 olmalidir");
        }

        if ((bytes == null) || (!(key instanceof Integer))) {
            throw new InvalidRecordException("Key mutleq Integer type olmalidir.");
        }

        if ((bytes == null) || (!(value instanceof Transaction))) {
            throw new InvalidRecordException("Value mutleq Transaction type olmalidir.");
        }

        /*
         * id#1 - partition 1 =userId % partitionsSize => 1 % 15 = 1;
         * id#2 - partition 2
         * id#3 - partition 3
         * --------------------------
         * id#14 - partition 14 => 14 % 15 = 14
         * id#15 - partition 15 => 15 % 15 = 0
         *
         * id#16 - partition 1
         * id#17 - partition 2
         * */

        Transaction transaction = (Transaction) value;

        return (int) (transaction.getUserId() % partitionSize);

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
