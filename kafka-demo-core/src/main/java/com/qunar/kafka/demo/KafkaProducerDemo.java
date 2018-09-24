package com.qunar.kafka.demo;

import kafka.Kafka;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;


/**
 * kafka demo
 *
 * @author jiabin.niu
 */
public class KafkaProducerDemo {

    private final Producer<String, String> kafkaProducer;
    public final static String TOPIC = "JAVA_TOPIC";

    public KafkaProducerDemo() {
        kafkaProducer = createKafkaProducer();
    }

    private Producer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "100.81.140.23:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 1024 * 1024 * 100);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        return kafkaProducer;
    }

    public static void main(String[] args) {
        new KafkaProducerDemo().produce();
    }

    private void produce() {
        for (int i = 1; i < 1000; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String key = String.valueOf("key" + i);
            String data = "hello kafka message:" + key;
            final Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(TOPIC, key, data), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) { //do sth

                }
            });
            System.out.println(data);
        }
    }
}
