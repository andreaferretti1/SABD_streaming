import time
import simplejson as json
import signal
import os

from utils.pandas_utils import *
from utils.kafka_utils import *

RUNNING = True

def shutdown(sig, frame):
    global RUNNING
    RUNNING = False

def generate_stream():

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)


    # Carico il dataset
    dataset = get_data()

    # Calcolo l'event time
    dataset['event_time'] = compute_event_time(dataset[['YEAR', 'MONTH', 'DAY_OF_MONTH', 'CRS_DEP_TIME']])

    # Ordino gli eventi in base all'event time
    dataset.sort_values('event_time', inplace = True)

    # Calcolo il fattore di accelerazione
    target_duration = os.getenv('STREAM_DURATION')
    target_duration = float(target_duration)
    speedup_factor = compute_speedup_factor(target_duration, dataset['event_time'])

    # Istanzio il producer kafka
    producer = get_producer()
    topic = "data"

    # Calcolo gli intertempi reali di trasmissione degli eventi
    event_times = dataset['event_time'].tolist()
    deltas = dataset['event_time'].diff()
    deltas.iloc[0] = 0
    deltas = deltas.tolist()

    # Elimino la colonna event_time per inviare i dati grezzi
    dataset.drop(columns = ['event_time'], inplace = True)
    dict_dataset = dataset.to_dict(orient='records')
    
    print(f"Tipo dato: {type(event_times[0])}")

    # Genero lo stream di eventi
    for i, record in enumerate(dict_dataset):

        if not RUNNING:
            break
        
        ts_seconds = event_times[i]
        ts = int(ts_seconds) * 1000

        if deltas[i] != 0:
            time.sleep(deltas[i] / speedup_factor)
        

        payload = json.dumps(record, ignore_nan = True)
        send_message(producer=producer, topic=topic, payload=payload, timestamp=ts)

        if i % 5000 == 0:
            producer.poll(0)

    if RUNNING:
        # Prendo lo Unix epoch time corrispondente al 1 giugno 2025
        end_timestamp = 1748736000000

        # Creo il messaggio dummy di fine stream
        end_of_stream_msg = json.dumps({"eos": "eos"})
        
        for partition in range(4):
            
            # Invio il messaggio
            send_message_with_partition(producer=producer, topic=topic, payload=end_of_stream_msg, timestamp=end_timestamp, partition = partition)

            # Flusho tutto su Kafka
            producer.flush()

    # Rilascio le risorse
    producer.close()











if __name__ == '__main__':
    generate_stream()
