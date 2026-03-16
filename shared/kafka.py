import json
from typing import Callable, Any

from confluent_kafka import Consumer, Producer

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

    @property
    def producer(self):
        if self._producer is None:
            self._producer = Producer({'bootstrap.servers': settings.KAFKA_BROKER})
        return self._producer

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
                    message_handler(msg_value)
                    self.consumer.commit(message=msg)
                    log_event("info", f"{settings.SERVICE_NAME} - successfully processing message")
                except Exception as e:
                    log_event("Error", f"{settings.SERVICE_NAME} - Error processing message: {e}")
        except KeyboardInterrupt:
            log_event("info", f"{settings.SERVICE_NAME} - Shutting down...")
        finally:
            self.consumer.close()

    def producer_message(self, next_event: dict[str, Any]) -> None:
        self.producer.poll(0)
        self.producer.produce(
            settings.PRODUCE_TOPIC,
            value=json.dumps(next_event).encode('utf-8')
        )
        self.producer.flush()


kafka_service = Kafka()
