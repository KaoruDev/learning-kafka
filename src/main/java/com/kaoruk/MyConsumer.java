package com.kaoruk;

import com.google.common.base.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyConsumer {
    private KafkaConsumer<String, String> consumer;
    private String name;

    public MyConsumer(Properties props, String name) {
        name = Optional.fromNullable(name).or("test_2");

        props.put("group.id", name);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("test"));
        this.name = name;
    }

    public void beginConsumming() {
       while (true) {
           ConsumerRecords<String, String> records = consumer.poll(100);
           System.out.println("Records...");

           for (ConsumerRecord<String, String> record: records) {

               System.out.println("offset: " + record.offset() +
                       " value: " + record.value() +
                       " key: " + record.key()
               );
           }

           try {
               Thread.sleep(1000);
           } catch (InterruptedException e) {
           }
       }
    }
}
