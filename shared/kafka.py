import json
from typing import Callable

from confluent_kafka import Consumer

from shared.config import settings
from shared.logger import log_event


class Kafka:
    def __init__(self):
        self._consumer = None
        self._producer = None

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = Consumer({
                'bootstrap.servers': settings.KAFKA_BROKER,
                'group.id': settings.GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
        return self._consumer

    def start_generic_consumer(self, message_handler: Callable):
        self.consumer.subscribe(settings.CONSUME_TOPIC.split(","))
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log_event("Error", f"Consumer error: {msg.error()}")
                    continue
                try:
                    msg_value = json.loads(msg.value().decode('utf-8'))
                    next_event = message_handler(msg_value)
                    self.consumer.commit(message=msg)
                except Exception as e:
                    log_event("Error", f"Error processing message: {e}")
        except KeyboardInterrupt:
            log_event("info", "Shutting down consumer...")
        finally:
            self.consumer.close()


kafka_service = Kafka()
