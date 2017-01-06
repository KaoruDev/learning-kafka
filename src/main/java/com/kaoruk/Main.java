package com.kaoruk;

import java.util.Properties;

/**
 * Created by kaoru on 1/6/17.
 */
public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        MyConsumer consumer = new MyConsumer(props, null);

        consumer.beginConsumming();
    }
}
