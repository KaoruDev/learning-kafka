package com.kaoruk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyProducer implements StatusListener {
    private Producer<String, String> producer;

    public void connect() {
        this.producer = new KafkaProducer<>(getProps());
    }

    public Properties getProps() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public void close() {
        producer.close();
    }

    @Override
    public void onStatus(Status status) {
        producer.send(new ProducerRecord<>(MyTwitter.TOPIC_NAME, status.getText()));
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {
        System.out.println("Something went wrong shutting down producer...");
        this.producer.close();
        ex.printStackTrace();
    }

    public static void main(String[] args) {
        MyTwitter.initialize();
        TwitterStream stream = new MyTwitter().getStream();
        MyProducer producer = new MyProducer();

        try {
            producer.connect();
            stream.addListener(producer);

            stream.sample();

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(20));
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        } finally {
            stream.cleanUp();
            producer.close();
        }
    }
}
