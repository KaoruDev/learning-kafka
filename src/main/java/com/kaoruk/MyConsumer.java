package com.kaoruk;

import com.google.common.base.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        props.put("group.id", "TwitterConsumptionB");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1_000);

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
        System.out.println(this.name + " is running!");
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
                System.out.println(this.name +
                        color(" partition: " + record.partition(), ThreadColor.ANSI_PURPLE) +
                        color(" offset: " + record.offset(), ThreadColor.ANSI_BLUE ) +
                        " timestamp: " + record.timestamp()
                );
//                System.out.println(this.name.toUpperCase() +
//                        " offset: " + record.offset() +
//                        " value: " + record.value()
//                );
            }

            consumer.commitAsync();
        }
        System.out.println(this.name + " STOPPED CONSUMMING!");
    }

    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        ReentrantLock lock = new ReentrantLock();

        List<MyConsumer> consumers = new ArrayList<>();

        consumers.add(new MyConsumer(color("Consumer_1", ThreadColor.ANSI_RED), lock));
        consumers.add(new MyConsumer(color("Consumer_2", ThreadColor.ANSI_GREEN), lock));
        consumers.add(new MyConsumer(color("Consumer_3", ThreadColor.ANSI_CYAN), lock));
        consumers.add(new MyConsumer(color("Consumer_4", ThreadColor.ANSI_YELLOW), lock));
        consumers.add(new MyConsumer(color("Consumer_5", ThreadColor.ANSI_BLUE), lock));
        consumers.add(new MyConsumer(color("Consumer_6", ThreadColor.ANSI_PURPLE), lock));

        consumers.forEach(pool::execute);
    }

    private static String color(String text, String color) {
        return color + text + ThreadColor.ANSI_RESET;
    }

    public class ThreadColor {
        public static final String ANSI_RESET = "\u001B[0m";
        public static final String ANSI_BLACK = "\u001B[30m";
        public static final String ANSI_RED = "\u001B[31m";
        public static final String ANSI_GREEN = "\u001B[32m";
        public static final String ANSI_YELLOW = "\u001B[33m";
        public static final String ANSI_BLUE = "\u001B[34m";
        public static final String ANSI_PURPLE = "\u001B[35m";
        public static final String ANSI_CYAN = "\u001B[36m";
        public static final String ANSI_WHITE = "\u001B[37m";
    }
}
