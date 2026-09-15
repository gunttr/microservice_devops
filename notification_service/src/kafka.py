from confluent_kafka import Consumer
from src.logger import logger
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
        self.__consumer.subscribe(["notifications"])
        
        try:
            while True:
                event = self.__consumer.poll(1.0)
                
                if event is None:
                    continue
                    
                if event.error():
                    logger.error(
                        f"Kafka consumer error: {event.error()}"
                    )
                    continue

                notification = json.loads(event.value().decode('utf-8'))
                send_notification(notification=notification)
            
        except:
            logger.error("Notification consumer crashed")
        finally:
            self.__consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "notification_service")
)