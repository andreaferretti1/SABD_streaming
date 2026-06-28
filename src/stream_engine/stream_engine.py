import time
import simplejson as json

from utils.pandas_utils import *
from utils.kafka_utils import *


def generate_stream():

    dataset = get_data()

    dataset['event_time'] = compute_event_time(dataset[['YEAR', 'MONTH', 'DAY_OF_MONTH', 'CRS_DEP_TIME']])

    dataset.sort_values('event_time', inplace = True)

    speedup_factor = compute_speedup_factor(10, dataset['event_time'])

    producer = get_producer()
    topic = "data"

    event_times = dataset['event_time'].to_numpy()
    deltas = dataset['event_time'].diff().to_numpy()
    deltas[0] = 0

    dataset.drop(columns = ['event_time'], inplace = True)
    dict_dataset = dataset.to_dict(orient='records')

    for i, record in enumerate(dict_dataset):

        if deltas[i] != 0:
            time.sleep(deltas[i] / speedup_factor)

        payload = json.dumps(record, ignore_nan = True)
        send_message(producer, topic, payload, event_times[i])

        if i % 5000 == 0:
            producer.poll(0)


    producer.flush()
    producer.close()











if __name__ == '__main__':
    generate_stream()