#!/bin/bash
set -e

export FLINK_PARALLELISM=$1
export RUN_ID=$2

COMPOSE_FILE="docker/docker-compose.yaml"

echo "Inizio deployment"

# Estraggo il dataset
echo "Estraggo il dataset"

if [ "$(ls -1q Dataset/*.csv 2>/dev/null | wc -l)" -eq 0 ]; then
    echo "Nessun file CSV trovato. Estraggo i file"
    tar -xzf ./Dataset/Dataset.tar.gz -C ./Dataset/
    echo "Estrazione completata"
else
    echo "File CSV già presenti"
fi

# Copio il contenuto di .env.template in .env
echo "Configuro variabili di ambiente per InfluxDB"
cat docker/.env.template > docker/.env

ENV_FILE="docker/.env"

get_env_var() {
    grep -E "^$1=" "$ENV_FILE" | head -n1 | cut -d '=' -f2-
}

# Nel tuo .env.template la variabile si chiama INFLUX_TOKEN
# L'organizzazione invece nel docker-compose è results
INFLUX_ORG="results"
INFLUX_TOKEN="$(get_env_var INFLUX_TOKEN | tr -d '\r' | sed 's/^"//; s/"$//; s/^'\''//; s/'\''$//' | xargs)"
if [ -z "$INFLUX_TOKEN" ]; then
    echo "ERRORE: INFLUX_TOKEN vuoto. Controlla docker/.env.template"
    exit 1
fi

echo "Variabili configurate"
echo "INFLUX_ORG=${INFLUX_ORG}"
echo "INFLUX_TOKEN=${INFLUX_TOKEN}"

# Avvio broker Kafka
echo "Avvio Kafka"

docker compose -f "$COMPOSE_FILE" up -d kafka

# Creo topic in cui salvare i risultati
echo "Creo i topic in cui scrivere i risultati"

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic data

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic q1-results

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic q2-results-1h

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic q2-results-6h

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --topic q2-results-global

echo "Topic creati"

echo "Avvio InfluxDB"
docker compose -f "$COMPOSE_FILE" up -d influxdb

echo "Attendo che InfluxDB sia pronto"

until docker exec influxdb influx ping > /dev/null 2>&1; do
    sleep 2
done

echo "InfluxDB pronto"

# Creo bucket flink_metrics e DBRP mapping
echo "Creo bucket flink_metrics e DBRP mapping"

docker exec influxdb influx bucket create \
    --name flink_metrics \
    --org "${INFLUX_ORG}" \
    --token "${INFLUX_TOKEN}" || true

echo "Primo step fatto"

BUCKET_ID=$(docker exec influxdb influx bucket list \
    --name flink_metrics \
    --org "${INFLUX_ORG}" \
    --token "${INFLUX_TOKEN}" \
    --json | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")

echo "BUCKET_ID=${BUCKET_ID}"
echo "Secondo step fatto"

docker exec influxdb influx v1 dbrp create \
    --db flink_metrics \
    --rp autogen \
    --bucket-id "${BUCKET_ID}" \
    --org "${INFLUX_ORG}" \
    --token "${INFLUX_TOKEN}" \
    --default || true

echo "Avvio i servizi rimanenti"
docker compose -f "$COMPOSE_FILE" up -d

rm -f Results/*.csv 2>/dev/null || true

echo "Tutti i servizi sono in esecuzione"
