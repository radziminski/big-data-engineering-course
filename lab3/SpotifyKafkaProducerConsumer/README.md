# Kafka Producer Consumer Example

Illustrates a simple example how to produce and consume messages.

## Pre-Install

Required

* Kafka Environment

For developing

* maven
* Java >=8 (best: Openjdk)
* IDE (e.g. Eclipse, ...)

## Before running the application directly

Before executing you need to add kafka and zookeeper to your OS hosts-file. Here is a list of hosts-file locations according to your operating systems

* **Mac OS:** /private/etc/hosts
* **Linux:** /etc/hosts
* **Windows:** C:\\Windows\\System32\\drivers\\etc\\hosts

The format may vary a little but usually a new host with its hostname is defined using it's **ip** and the desired **hostname**. Thus, your host file needs to have the following entries:

```table
127.0.0.1  kafka
127.0.0.1  zookeeper
```

Be aware that for some Operating Systems you might need to restart your network service or restart your computer. You may check if the entries are active by just pining with `ping kafka` and `ping zookeeper`, which should give you a response from 127.0.0.1.

## Using and building the Applications

To build the project execute:

```bash
mvn clean install
```

To run a consumer:

```bash
mvn exec:java@consumer
```

To run the producer:

```bash
mvn exec:java@producer
```

## Helpers

* [Consumer API Reference](https://kafka.apache.org/21/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
* [Producer API Reference](https://kafka.apache.org/21/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
* [Overall API Reference](https://kafka.apache.org/21/javadoc/)

## Dockerize

The project may run in a docker container. When used in a docker-compose file the following environment variables are available:

```yaml
environment:
    # kafka relatead
    KAFKA_BOOTSTRAP_SERVERS: "broker:29092"
    KAFKA_DEFAULT_TOPIC: "iottopicavro"
    # message amount
    PRODUCER_SEND_MESSAGES_COUNT: 1500
    PRODUCER_SENSOR_COUNT: 15
    SCHEMA_REGISTRY_URL_CONFIG: 'http://schema-registry:8081'
    # decide on how fast to deliver the messages
    PRODUCER_DELAY_FACTOR_MSG_POSTS: 30
```

When configured in `docker-compose.yml` file with the environment variables defined like above the container can be build with the following command asuming that the service name is called `iot-producer`.

```bash
docker-compose build iot-producer
```