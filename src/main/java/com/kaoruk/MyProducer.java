package com.kaoruk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyProducer {
    public MyProducer(Properties props) {
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(Producer<String, String> producer = new KafkaProducer<String, String>(props);) {
            for (int i = 0; i < 100; i++) {
                System.out.println("Sending...message: " + i);
                producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "This must be the value!"));
            }
        }
    }
}
