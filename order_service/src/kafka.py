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
                msg="Failed to produce kafka event",
                extra={
                    "topic": msg.topic(),
                    "key": msg.key().decode('utf-8'),
                    "value": json.loads(msg.value().decode('utf-8')),
                    "error": str(err)
                }
            )
        else:
            logger.info(
                msg="Kafka event produced successfully",
                extra={
                    "topic": msg.topic(),
                    "key": msg.key().decode('utf-8'),
                    "value": json.loads(msg.value().decode('utf-8'))
                }
            )


    def consume_payments(self):
        consume_topic: str = "payments"
        self.__kafka_consumer.subscribe([consume_topic])
        
        try:
            while True:
                event = self.__kafka_consumer.poll(1.0)
                
                if event is None:
                    continue
                    
                if event.error():
                    logger.error(
                        msg="Kafka failed to consume",
                        extra={
                            "topic": consume_topic,
                            "error": str(event.error())
                        }
                    )
                    continue

                payment = json.loads(event.value().decode('utf-8'))
                pgsql_manager.update_order_status(payment=payment)
        except Exception as err:
            logger.error(
                msg="Kafka consumer crashed",
                extra={
                    "topic": consume_topic,
                    "error": err
                }
            )
        finally:
            self.__kafka_consumer.close()

    # отправляем notification_service
    def produce_notifications(self, order_id: int, order_username: str):
        self.__kafka_producer.produce(
            topic="notifications",
            key="order",
            value=json.dumps({
                    "order_id": order_id,
                    "username": order_username
                }),
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