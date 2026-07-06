#!/bin/bash

COMPOSE_FILE="docker/docker-compose.yaml"

# Elimino producer e consumer
docker container stop jobmanager taskmanager csv_consumer influxdb_consumer stream_engine

# Elimino topic kafka
echo "Elimino topic Kafka"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic data \
    --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic q1_results \
    --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic q2_results_1h \
    --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic q2_results_6h \
    --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic q2_results_global \
    --bootstrap-server localhost:9092

echo "Topic eliminati"

echo "Spengo e rimuovo container"
docker compose -f $COMPOSE_FILE down

echo "Teardown completato"