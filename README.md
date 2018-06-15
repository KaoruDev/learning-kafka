# Learning Kafka

This repo is a simple Twitter Stream => Kafka Producer => Kafka Consumer. This is not meant for production use. The main use case for this repo is to learn Kafka's java APIs.


### Getting started
1. Create a Twitter app by going to: https://apps.twitter.com/
2. Copy keys and create a json file called `.secrets.json` which looks like:
```
{
  "twitter": {
    "access_token": "your_access_token_here",
    "access_token_secret": "you_access_token_secret_here",
    "consumer_key": "your_consumer_key_here",
    "consumer_key_secret": "your_consumer_key_secret_here"
  }
}
```
3. Follow Kafka's Quickstart: https://kafka.apache.org/quickstart guide to get ZK and Kafka running.
4. Create topic:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 9 --topic TwitterStream1
```
Make sure the topic name is the same as `com.kaoruk.MyTwitter.TOPIC_NAME`. Make sure you have enough partitions setup so you do not starve your Consumers.
5. Start Producer to start sending messages, start Consumer to see the output.
6. Win.
