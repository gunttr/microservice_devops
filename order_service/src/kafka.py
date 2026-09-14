from confluent_kafka import Producer, Consumer
import json
from src.logger import logger
from src.sql import pgsql_manager
import os


class KafkaManager:
    def __init__(self, hostname: str, port: str, group_id: str) -> None:
        self.__kafka_consumer: Consumer = Consumer({
            "bootstrap.servers": f"{hostname}:{port}",
            "group.id": group_id
        })
        self.__kafka_producer: Producer = Producer({
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


    def consume_payments(self):
        self.__kafka_consumer.subscribe(["payments"])
        
        try:
            while True:
                event = self.__kafka_consumer.poll(1.0)
                
                if event is None:
                    continue
                    
                if event.error():
                    logger.error(
                        f"Kafka consumer error: {event.error()}"
                    )
                    continue

                payment = json.loads(event.value().decode('utf-8'))
                pgsql_manager.update_order_status(payment=payment)
        except:
            logger.error("Payment consumer crashed")
        finally:
            self.__kafka_consumer.close()

    # отправляем notification_service
    def produce_notifications(self, order_id: int, order_username: str):
        self.__kafka_producer.produce(
            topic="notifications",
            key="order",
            value=json.dumps(
                {
                    "order_id": order_id,
                    "username": order_username
                }
            ),
            callback=self.__kafka_delivery_callback
        )
    
        self.__kafka_producer.flush()

    # отправляем payment_service
    def produce_orders(self, order_id: int):
        self.__kafka_producer.produce(
            topic="orders",
            key="order_id",
            value=str(order_id),
            callback=self.__kafka_delivery_callback
        )

        self.__kafka_producer.flush()


    def close_consumer(self):
        self.__kafka_consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "order_service")
)