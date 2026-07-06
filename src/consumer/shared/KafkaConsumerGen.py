import signal
from confluent_kafka import Consumer, KafkaError, KafkaException


class KafkaConsumerGen:

    def __init__ (self, group_id, routing_table):
        self.group_id = group_id
        self.running = True
        self.routing_table = routing_table
        self.consumer = self._get_consumer()

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)


    # Fermo il polling per consentire il corretto rilascio delle risorse
    def _shutdown(self, sig, frame):
        self.running = False


    # Istanzia consumer
    def _get_consumer(self):

        config = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }

        return Consumer(config)


    # Esegue il polling per scaricare i risultati
    def consuming_loop(self):

        topics = list(self.routing_table.keys())
        try:
            self.consumer.subscribe(topics)

            while self.running:
                msg = self.consumer.poll(timeout = 1.0)

                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        pass
                    else:
                        raise KafkaException(msg.error())
                else:
                    topic = msg.topic()
                    self.routing_table[topic](msg)

        finally:
            self.consumer.close()