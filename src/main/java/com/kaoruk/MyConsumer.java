package com.kaoruk;

import com.google.common.base.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyConsumer implements Runnable{
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private ReentrantLock lock;
    private String name;

    public MyConsumer(String name, ReentrantLock lock) {
        name = Optional.fromNullable(name).or("default");
        this.name = name;
        this.lock = lock;

        consumer = new KafkaConsumer<String, String>(getProps());
        consumer.subscribe(Arrays.asList(MyTwitter.TOPIC_NAME));
    }

    private Properties getProps() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("group.id", name);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    public void shutdown() {
        try {
            lock.lock();
            running = false;
            consumer.close();
            System.out.println("SHUTTING DOWN CONSUMER " + this.name);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        while (running) {
            ConsumerRecords<String, String> records;
            try {
                lock.lock();
                if (running) {
                    records = consumer.poll(100);
                } else {
                    break;
                }
            } finally {
                lock.unlock();
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(this.name.toUpperCase() +
                        " offset: " + record.offset() +
                        " key: " + record.key() +
                        " value: " + record.value()
                );
            }
        }
        System.out.println(this.name + " STOPPED CONSUMMING!");
    }
}
