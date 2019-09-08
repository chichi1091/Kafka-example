package com.tera.kafka_exmaple.sender;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class TopicSyncSendProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myProducerGroup");

        try(KafkaProducer<String, String> producer =
                    new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
            String key = UUID.randomUUID().toString();
            String value = "hello world";
            Date date = new Date(System.currentTimeMillis() + 1000 * 60 * 3);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("my-topic", null, date.getTime(), key, value);

            // sync send
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("=============================");
            System.out.println(LocalDateTime.now());
            System.out.println("topic: " + recordMetadata.topic());
            System.out.println("partition: " + recordMetadata.partition());
            System.out.println("offset: " + recordMetadata.offset());
            System.out.println("timestamp:" + new Date(recordMetadata.timestamp()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final String key;
    private final String message;

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
     * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
