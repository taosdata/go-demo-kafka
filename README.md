# TDengine Demo Series - Consume Messages from Kafka

## Kafka Setup

```sh
docker-compose up -d kafka
```

## Create Topic

```sh
docker-compose exec kafka \
  kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1\
    --partitions 1 \
    --topic test
# check topic list
docker-compose exec kafka \
  kafka-topics.sh --list \
  --zookeeper zookeeper:2181
```

## Producer and Consumer Test using internal client

```sh
docker-compose exec kafka \
  kafka-console-producer.sh --broker-list kafka:9092 --topic test
docker-compose exec kafka \
  kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test --from-beginning
```

![internal-client-test](assets/internal-client-test.png)

## Go Producer and Consumer

First we start tdengine in docker-compose

```sh
docker-compose up -d tdengine
```

Create test database

```sh
docker-compose exec tdengine -c "create database if not exists test"
```

Build go producer and consumer.

```sh
go build cmd/producer/producer.go 
go build cmd/consumer/consumer.go
```

Run consumer.

```sh
docker-compose run --rm consumer
```

Run producer.

```sh
docker-compose run --rm producer
```

The results in console:

![go-produce-consume](assets/go-produce-consume.png)
