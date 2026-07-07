import json
from influxdb_client import Point

WINDOW_SIZES = {
    "q2-results-1h": "1",
    "q2-results-6h": "6",
    "q2-results-global": "global"}

def process_q2_result(msg, write_api, bucket):

    try:

     # Deserializzo il messaggio
     top10 = json.loads(msg.value())
     topic = msg.topic()
     window_size = WINDOW_SIZES[topic]

     points = []

     for airport in top10:

        # Definisco il punto
        airport_point =( Point("q2_result")
                .tag("window_size", window_size)
                .tag("airport", airport["origin_airport_id"])
                .field("rank", airport["rank"])
                .field("num_flights", airport["num_flights"])
                .field("severe_delays", airport["severe_delays"])
                .time(airport["ts"], write_precision="ms")
        )

        if airport["dep_delay_mean"] is not None:
            airport_point.field("dep_delay_mean", airport["dep_delay_mean"])
        if airport["dep_delay_max"] is not None:
            airport_point.field("dep_delay_max", airport["dep_delay_max"])

        points.append(airport_point)

        # Creo un punto per ciascun aereo partito in ritardo
        delayed_flights = airport['delayed_flights']
        for rank, flight in enumerate(delayed_flights, start=1):
            flight_point = (Point("delayed_flights")
                            .tag("window_size", window_size)
                            .tag("origin_airport", airport["origin_airport_id"])
                            .tag("rank", str(rank))
                            .field("airline", flight["carrier"])
                            .field("dest_airport", flight["dest_airport"])
                            .field("dep_delay", float(flight["dep_delay"]))
                            .time(airport["ts"], write_precision="ms")
                            )
            points.append(flight_point)

    # Scrivo sul data store
     write_api.write(bucket=bucket, record=points)



    except Exception as e:
        print(f"Errore durante la scrittura del risultato di q2 su influxDB: {e}")
