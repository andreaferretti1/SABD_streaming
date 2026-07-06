import json
import os
from datetime import datetime, timezone


CSV_PATH = "Results/query1.csv"

# Scrive l'header nel file
def setup_q1():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'w') as file:
            file.write("window_start,window_end,airline,num_flights,completed,cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate\n")


def process_q1_result(msg):

        # Estraggo il payload del messaggio
        payload = json.loads(msg.value())

        # Converto le date nel formato richiesto
        dt = datetime.fromtimestamp(payload['window_start'] / 1000, tz=timezone.utc)
        formatted_window_start = dt.strftime('%Y-%m-%d %H:%M:%S')

        dt = datetime.fromtimestamp(payload['window_end'] / 1000, tz=timezone.utc)
        formatted_window_end = dt.strftime('%Y-%m-%d %H:%M:%S')

        # Formatto dep_delay_mean
        dep_delay_mean = payload['dep_delay_mean']
        formatted_dep_delay_mean = f"{dep_delay_mean:.2f}" if dep_delay_mean is not None else "null"

        # Formatto late_departure_rate
        late_departure_rate = payload['late_departure_rate']
        formatted_late_departure_rate = f"{late_departure_rate:.2f}" if late_departure_rate is not None else "null"

        # Formatto la riga CSV da salvare
        csv_record = (f"{formatted_window_start},{formatted_window_end},{payload['airline']},{payload['num_flights']},"
                      f"{payload['completed']},{payload['cancelled']},{payload['diverted']},{formatted_dep_delay_mean},"
                      f"{payload['cancellation_rate']:.2f},{formatted_late_departure_rate}\n")

        # Scrivo la riga nel file
        with open(CSV_PATH, 'a') as file:
            file.write(csv_record)
            file.flush()
