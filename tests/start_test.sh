#!/bin/bash

# Configurazione percorsi e parametri
COMPOSE_PATH="../compose/docker-compose.yaml"
KAFKA_TOPICS=("data" "q1-results" "q2-results-1h" "q2-results-6h" "q2-results-global")

export FLINK_PARALLELISM=$1
export RUN_ID=$2

echo "--- Parallelismo: $FLINK_PARALLELISM, Run: $RUN_ID ---"

# Avvio InfluxDB e Kafka
docker compose -f ${COMPOSE_PATH} up -d influxdb kafka

# Creo topic Kafka
echo "Creo topic Kafka"
for TOPIC in "${KAFKA_TOPICS[@]}"; do
    echo " -> Creazione: $TOPIC"
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
        --topic $TOPIC \
        --bootstrap-server localhost:9092 \
        --partitions 4
done

# Creo bucket e faccio mapping dbrp
docker exec influxdb influx bucket create \
    --name flink_metrics \
    --org results \
    --token ${INFLUX_TOKEN}

BUCKET_ID=$(docker exec influxdb influx bucket list \
    --name flink_metrics \
    --org results \
    --token ${INFLUX_TOKEN} \
    --json | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")

docker exec influxdb influx v1 dbrp create \
    --db flink_metrics \
    --rp autogen \
    --bucket-id ${BUCKET_ID} \
    --org results \
    --token ${INFLUX_TOKEN} \
    --default

# Avvio Flink e stream engine
echo "Avvio Flink e stream engine"
docker compose -f ${COMPOSE_PATH} up -d flink-job_manager flink-task_manager stream_engine

echo "Job avviato"
