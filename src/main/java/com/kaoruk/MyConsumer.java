package com.kaoruk;

import com.google.common.base.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyConsumer implements Runnable{
    private KafkaConsumer<String, String> consumer;
    private AtomicBoolean running = new AtomicBoolean(true);
    private String name;

    public MyConsumer(String name) {
        name = Optional.fromNullable(name).or("default");
        this.name = name;

        consumer = new KafkaConsumer<String, String>(getProps());
        consumer.subscribe(Arrays.asList(MyTwitter.TOPIC_NAME));
    }

    private Properties getProps() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("group.id", "TwitterConsumptionB");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(1));

        return props;
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void run() {
        System.out.println(this.name + " is running!");
        ConsumerRecords<String, String> records;

        try {
            while (running.get()) {
                records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {

                    Headers headers = record.headers();
                    headers.forEach((header) -> {
                        System.out.println("Header key: " + header.key() +
                            " value: " +  new String(header.value(), StandardCharsets.UTF_8));
                    });

                    System.out.println(this.name +
                            color(" partition: " + record.partition(), ThreadColor.ANSI_PURPLE) +
                            color(" offset: " + record.offset(), ThreadColor.ANSI_BLUE) +
                            " timestamp: " + record.timestamp()
                    );

//                System.out.println(this.name.toUpperCase() +
//                        " offset: " + record.offset() +
//                        " value: " + record.value()
//                );
                }
            }
        } catch (WakeupException e) {
            System.out.println("Wake up called, SHUTTING DOWN CONSUMER " + this.name);
        } finally {
            consumer.close();
            System.out.println(this.name + " STOPPED CONSUMMING!");
        }
    }

    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<MyConsumer> consumers = new ArrayList<>();

        consumers.add(new MyConsumer(color("Consumer_1", ThreadColor.ANSI_RED)));
        consumers.add(new MyConsumer(color("Consumer_2", ThreadColor.ANSI_GREEN)));
        consumers.add(new MyConsumer(color("Consumer_3", ThreadColor.ANSI_CYAN)));
        consumers.add(new MyConsumer(color("Consumer_4", ThreadColor.ANSI_YELLOW)));
        consumers.add(new MyConsumer(color("Consumer_5", ThreadColor.ANSI_BLUE)));
        consumers.add(new MyConsumer(color("Consumer_6", ThreadColor.ANSI_PURPLE)));

        consumers.forEach(pool::execute);

        try {
            Thread.sleep(TimeUnit.MINUTES.toMillis(5));
        } catch (InterruptedException e) {
            System.out.println("Sleep was interrupted for whatever reason");
        }

        consumers.forEach(MyConsumer::shutdown);
        pool.shutdown();
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
