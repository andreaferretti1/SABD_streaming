#!/bin/bash

export FLINK_PARALLELISM=$1
export RUN_ID=$2

COMPOSE_FILE="docker/docker-compose.yaml"

echo "Inizio deployment"

# Estraggo il dataset
echo "Estraggo il dataset"
# Conta i file .csv presenti nella cartella Dataset.
# Se ce n'è almeno uno, diamo per scontato che sia già stato estratto.
if  [ "$(ls -1q Dataset/*.csv | wc -l)" -eq 0 ]; then
    echo "Nessun file CSV trovato. Estraggo i file"
    tar -xzf ./Dataset/Dataset.tar.gz -C ./Dataset/
    echo "Estrazione completata"
else
    echo "File CSV già presenti"
fi

# Copio il contenuto di .env.template in .env
echo "Configuro variabili di ambiente per InfluxDB"
cat docker/.env.template > docker/.env
source docker/.env
echo "Variabili configurate"

# Avvio broker Kafka
echo "Avvio Kafka"
# Avviamo tutto tranne i due consumer
docker compose -f $COMPOSE_FILE up -d kafka

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
docker compose -f $COMPOSE_FILE up -d influxdb
sleep 10

# Creo bucket flink_metrics e DBRP mapping
echo "Creo bucket flink_metrics e DBRP mapping"
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


echo "Avvio i servizi rimanenti"
docker compose -f $COMPOSE_FILE up -d

echo "Tutti i servizi sono in esecuzione"