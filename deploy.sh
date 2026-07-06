#!/bin/bash

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
    --partitions 1 \
    --replication-factor 1 \
    --topic data

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic q1_results

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic q2_results_1h

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic q2_results_6h

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic q2_results_global

echo "Topic creati"

# Avvio i servizi rimanenti
echo "Avvio i servizi rimanenti"
docker compose -f $COMPOSE_FILE up -d

echo "Tutti i servizi sono in esecuzione"