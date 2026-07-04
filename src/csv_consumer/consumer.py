import signal

from confluent_kafka import Consumer, KafkaError, KafkaException
from csv_consumer.process_results.process_q1_result import *
from csv_consumer.process_results.process_q2_result import *

# Tabella per invocare la funzione di processamento corretta
ROUTING_TABLE = {
    "q1_results": process_q1_result,
    "q2_results_1h": process_q2_result,
    "q2_results_6h": process_q2_result,
    "q2_results_global": process_q2_result
}

RUNNING = True


# Ferma il polling per consentire il corretto rilascio delle risorse
def shutdown(sig, frame):

    global RUNNING
    RUNNING = False

# Istanzia consumer
def get_consumer(group_id):

    config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    return Consumer(config)



# Esegue il polling per scaricare i risultati
def consuming_loop(consumer, topics):

    try:
        consumer.subscribe(topics)

        while RUNNING:
            msg = consumer.poll(timeout = 1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    raise KafkaException(msg.error())
            else:
                topic = msg.topic()
                ROUTING_TABLE[topic](msg)

    finally:
        consumer.close()






def main():
    os.makedirs('Results', exist_ok=True)

    setup_q1()
    setup_q2()

    consumer = get_consumer("csv_consumer")
    topics = [topic for topic in ROUTING_TABLE.keys()]

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    consuming_loop(consumer, topics)


if __name__ == "__main__":
    main()