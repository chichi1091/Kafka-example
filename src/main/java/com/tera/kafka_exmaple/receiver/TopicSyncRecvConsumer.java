package com.tera.kafka_exmaple.receiver;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

public class TopicSyncRecvConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myProducerGroup");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Collections.singletonList("my-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));

                if (records.count() > 0) {
                    System.out.println("=============================");
                    System.out.println("[record size] " + records.count());
                }
                records.forEach(record -> {
                    System.out.println("=============================");
                    System.out.println(LocalDateTime.now());
                    System.out.println("topic: " + record.topic());
                    System.out.println("partition: " + record.partition());
                    System.out.println("key: " + record.key());
                    System.out.println("value: " + record.value());
                    System.out.println("offset: " + record.offset());
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
                    if (offsetAndMetadata != null) {
                        System.out.println("partition offset: " + offsetAndMetadata.offset());
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
