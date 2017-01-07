package com.kaoruk;

import twitter4j.TwitterStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kaoru on 1/6/17.
 */
public class Main {
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        ReentrantLock lock = new ReentrantLock();

        MyTwitter.initialize();
        TwitterStream stream = new MyTwitter().getStream();
        MyProducer producer = new MyProducer();
        List<MyConsumer> consumers = new ArrayList<>();

        consumers.add(new MyConsumer("Consumer_1", lock));
        consumers.add(new MyConsumer("Consumer_2", lock));
        consumers.add(new MyConsumer("Consumer_3", lock));

        try {
            producer.connect();
            stream.addListener(producer);

            stream.sample();

            consumers.forEach(pool::execute);

            try {
                Thread.sleep(10_000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        } finally {
            System.out.println("SHUTTING DOWN.....");
            System.out.println("Consumers...");
            consumers.forEach(MyConsumer::shutdown);
            System.out.println("Pool...");
            pool.shutdown();
            System.out.println("Stream...");
            stream.cleanUp();
            System.out.println("Producer...");
            producer.close();
            System.out.println("Producer is now closed");
        }
    }
}
