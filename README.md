# kafka-talk
Talk on: How do we use Apache Kafka at Swisscom and what is it for?
https://www.youtube.com/watch?v=leAMaissdVo

## What is it?
It contains the presentation and code which I used during Swisscom DevOps week for the talk about Kafka.
The simple examples of setting up Kafka altogether with Spring Boot and Kafka Streams.



## Start Kafka
Run Kafka locally by typing:
docker-compose -f kafka.yml up

## Access Kafka
1. Get container ID by: docker ps
2. Access image by: docker exec -it <container_id>  bash

## Create topic
1. Access Kafka
2. kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic <topic_name>

## List topics
1. Access Kafka
2. 

## Produce message
kafka-console-producer --broker-list localhost:9092  --topic test --property "parse.key=true" --property "key.separator=:"

## Consumer messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning

## List topics
kafka-topics --list --bootstrap-server localhost:9092
