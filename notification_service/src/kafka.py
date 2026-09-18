from confluent_kafka import Consumer
from src.otel.logger import logger
import json
from src.notification import send_notification
import os


class KafkaManager:
    def __init__(self, hostname: str, port: str, group_id: str) -> None:
        self.__consumer = Consumer({
            "bootstrap.servers": f"{hostname}:{port}",
            "group.id": group_id
        })

    def close_consumer(self):
        self.__consumer.close()


    def consume_notifications(self):
        consume_topic: str = "notifications"
        self.__consumer.subscribe([consume_topic])
        
        try:
            while True:
                event = self.__consumer.poll(1.0)
                
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

                notification = json.loads(event.value().decode('utf-8'))
                send_notification(notification=notification)
            
        except Exception as err:
            logger.error(
                msg="Kafka consumer crashed",
                extra={
                    "topic": consume_topic,
                    "error": str(err)
                }
            )
        finally:
            self.__consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "notification_service")
)