#!/bin/bash

# Configurazione percorsi e parametri
COMPOSE_PATH="../compose/docker-compose.yaml"
KAFKA_TOPICS=("data" "q1-results" "q2-results-1h" "q2-results-6h" "q2-results-global")

# Verifica che le variabili di ambiente siano state esportate
if [ -z "$FLINK_PARALLELISM" ] || [ -z "$RUN_ID" ]; then
    echo "ERRORE: Devi esportare FLINK_PARALLELISM e RUN_ID prima di lanciare lo script."
    echo "Esempio: export FLINK_PARALLELISM=4; export RUN_ID=1; ./start_test.sh"
    exit 1
fi

echo "--- Avvio container Parallelismo: $FLINK_PARALLELISM, Run: $RUN_ID ---"

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

# Avvio Flink e stream engine
echo "Avvio Flink e stream engine"
docker compose -f ${COMPOSE_PATH} up -d flink-job_manager flink-task_manager stream_engine

echo "Job avviato"
