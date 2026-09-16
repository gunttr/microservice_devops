from confluent_kafka import Consumer, Producer
from src.logger import logger
import json
from src.payment import PaymentStatus
from src.payment import process_payments
import os


class KafkaManager:
    def __init__(self, hostname: str, port: str, group_id: str) -> None:
        self.__kafka_consumer = Consumer({
            "bootstrap.servers": f"{hostname}:{port}",
            "group.id": group_id
        })
        self.__kafka_producer = Producer({
            "bootstrap.servers": f"{hostname}:{port}",
        })

    def __kafka_delivery_callback(self, err, msg):
        if err:
            logger.error(
                f"Event failed delivery: {err}"
            )
        else:
            logger.info(
                f"Produced event to topic {msg.topic()}: "
                f"key = {msg.key().decode('utf-8')}; "
                f"value = {msg.value().decode('utf-8')}"
            )


    def consume_orders(self):
        self.__kafka_consumer.subscribe(["orders"])

        try:
            while True:
                event = self.__kafka_consumer.poll(1.0)

                if not event:
                    continue

                if event.error():
                    logger.error(
                        f"Kafka consumer error: {event.error()}"
                    )
                    continue
        
                order_id: int = int(event.value().decode('utf-8'))
                status: PaymentStatus = process_payments(order_id=order_id)
                self.__kafka_producer.produce(
                    topic="payments",
                    key="payment",
                    value=json.dumps({
                        "order_id": order_id,
                        "status": status.value
                    }).encode('utf-8'),
                    callback=self.__kafka_delivery_callback
                )
                self.__kafka_producer.flush()
        finally:
            self.__kafka_consumer.close()


    def consumer_close(self):
        self.__kafka_consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "payment_service")
)