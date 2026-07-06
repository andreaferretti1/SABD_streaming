import json
from influxdb_client import Point

def process_q1_result(msg, write_api, bucket):

    try:

        # Deserializzo il messaggio
        payload = json.loads(msg.value())

        # Creo il punto
        point = (Point("q1_result")
        .tag("airline", payload['airline'])
        .field("num_flights", int(payload['num_flights']))
        .field("completed", int(payload['completed']))
        .field("cancelled", int(payload['cancelled']))
        .field("diverted", int(payload['diverted']))
        .field("cancellation_rate", float(payload['cancellation_rate']))
        )

        if payload.get('dep_delay_mean') is not None:
            point.field("dep_delay_mean", float(payload['dep_delay_mean']))

        if payload.get('late_departure_rate') is not None:
            point.field("late_departure_rate", float(payload['late_departure_rate']))

        # Aggiungo il timestamp, costituito dal timestamp di chiusura della finestra
        point.time(payload['window_end'], write_precision="ms")

        # Scrivo nel data store
        write_api.write(bucket=bucket, record=point)

    except Exception as e:
        print(f"Errore durante la scrittura del risultato di q1 su influxDB: {e}")