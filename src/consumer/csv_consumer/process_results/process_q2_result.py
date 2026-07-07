import json
import os
from datetime import datetime, timezone

CSV_PATHS = {"q2-results-1h":"Results/query2_1h.csv", "q2-results-6h":"Results/query2_6h.csv", "q2-results-global": "Results/query2_global.csv"}

# Scrive l'header nei file
def setup_q2():
    for _, path in CSV_PATHS.items():
        if not os.path.exists(path):
            with open(path, 'w') as file:
                file.write(
                   "ts,rank,origin_airport_id,num_flights,severe_delays,dep_delay_mean,dep_delay_max,delayed_flights\n")



def process_q2_result(msg):
    
    topic = msg.topic()

    top10 = json.loads(msg.value())

    with open(CSV_PATHS[topic], 'a') as file:

        for airport in top10:

            # Formatto il timestamp di inizio finestra
            dt = datetime.fromtimestamp(airport['ts'] / 1000, tz=timezone.utc)
            formatted_window_start = dt.strftime('%Y-%m-%d %H:%M:%S')

            # Formatto la lista dei voli in ritardo
            delayed_flights = airport['delayed_flights']
            formatted_delayed_flights = [f"({flight['carrier']}, {flight['dest_airport']}, {flight['dep_delay']:.2f})" for flight in delayed_flights]
            formatted_delayed_flights = "\"[" + ",".join(formatted_delayed_flights) + "]\""

            # Formatto dep_delay_mean
            dep_delay_mean = airport['dep_delay_mean']
            formatted_dep_delay_mean = f"{dep_delay_mean:.2f}" if dep_delay_mean is not None else "null"

            # Formatto dep_delay_max
            dep_delay_max = airport['dep_delay_max']
            formatted_dep_delay_max = f"{dep_delay_max:.2f}" if dep_delay_max is not None else "null"

            # Formatto il record csv
            csv_record = (f"{formatted_window_start},{airport['rank']},{airport['origin_airport_id']},{airport['num_flights']},{airport['severe_delays']},{formatted_dep_delay_mean},"
                          f"{formatted_dep_delay_max},{formatted_delayed_flights}\n")

            # Scrivo il record nel file
            file.write(csv_record)
            file.flush()
