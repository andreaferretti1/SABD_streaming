from functools import partial
from consumer.influxdb_consumer.process_results.process_q1_result import process_q1_result
from consumer.influxdb_consumer.process_results.process_q2_result import process_q2_result
from consumer.shared.KafkaConsumerGen import KafkaConsumerGen
from init_influxdb import init_influxdb


def main():

    # Creo le api per scrivere sul data store
    influx_client, write_api = init_influxdb()

    # Tabella per invocare la funzione di processamento corretta
    routing_table = {
        "q1_results": partial(process_q1_result, write_api=write_api, bucket="q1_results"),
        "q2_results_1h": partial(process_q2_result, write_api=write_api, bucket="q2_results"),
        "q2_results_6h": partial(process_q2_result, write_api=write_api, bucket="q2_results"),
        "q2_results_global": partial(process_q2_result, write_api=write_api, bucket="q2_results")
    }

    # Istanzio il consumer
    consumer_gen = KafkaConsumerGen("influxdb_consumer", routing_table)

    # Avvio polling
    consumer_gen.consuming_loop()

    # Rilascio risorse
    write_api.close()
    influx_client.close()

if __name__ == "__main__":
    main()