#!/bin/bash

COMPOSE_PATH="../compose/docker-compose.yaml"
KAFKA_TOPICS=("data" "q1-results" "q2-results-1h" "q2-results-6h" "q2-results-global")

echo "Dealloco risorse"

for TOPIC in "${KAFKA_TOPICS[@]}"; do

    docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete \
        --topic $TOPIC \
        --bootstrap-server localhost:9092
done

# Rimuovo container
echo "Rimuovo container Kafka, Flink e stream engine"
docker compose -f ${COMPOSE_PATH} down kafka flink-job_manager flink-task_manager stream_engine

echo "Container distrutti"
