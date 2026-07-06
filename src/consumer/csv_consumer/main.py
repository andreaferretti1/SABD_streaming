import os

from consumer.csv_consumer.process_results.process_q1_result import setup_q1, process_q1_result
from consumer.csv_consumer.process_results.process_q2_result import setup_q2, process_q2_result
from consumer.shared.KafkaConsumerGen import KafkaConsumerGen




def main():
    os.makedirs('Results', exist_ok=True)

    # Tabella per invocare la funzione di processamento corretta
    routing_table = {
        "q1_results": process_q1_result,
        "q2_results_1h": process_q2_result,
        "q2_results_6h": process_q2_result,
        "q2_results_global": process_q2_result
    }

    # Scrivo gli header nei file csv
    setup_q1()
    setup_q2()

    # Avvio il consumer
    consumer_gen = KafkaConsumerGen("csv_consumer", routing_table)

    # Inizio polling
    consumer_gen.consuming_loop()


if __name__ == "__main__":
    main()