from confluent_kafka import Producer

# Questa funzione istanzia un producer
def get_producer():
    config = {
        'bootstrap.servers': 'kafka:9092',
        'acks': 1,
        # retries lasciato a 2147483647
        # linger.ms lasciato a 5ms
        # queued.max.messages.kbytes lasciato a 64MB
        # batch.size lasciato a 10000
    }

    return Producer(config)

# Questa funzione definisce la callback da chiamare dopo l'invio del record al broker
def callback(err, msg):
    if err is not None:
        print(f"Errore: {err}, Topic: {msg.topic()}, Timestamp: {msg.timestamp()}")


# Questa funzione invia un messaggio
def send_message(producer, topic, payload, timestamp):
    
    ts = int(timestamp)
    if not hasattr(send_message, '_count'):
        send_message._count= 0
    if send_message._count<5:
        print(f'Debug ts = {ts}, type={type(ts)}, len={len(str(ts))}', flush = True)
    producer.produce(topic = topic, value = payload, timestamp = int(timestamp), on_delivery = callback)



def send_message_with_partition(producer, topic, payload, timestamp, partition):
    ts = int(timestamp)
    if not hasattr(send_message, '_count'):
        send_message._count= 0
    if send_message._count<5:
        print(f'Debug ts = {ts}, type={type(ts)}, len={len(str(ts))}', flush = True)
    producer.produce(topic = topic, value = payload, timestamp = int(timestamp), on_delivery = callback, partition = partition)
