import json
import os

from confluent_kafka import Consumer
from opentelemetry.propagate import extract as trace_extract
from src.notification import send_notification
from src.otel.logger import logger
from src.otel.traces import tracer


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

                headers = {}
                if event_headers := event.headers():
                    headers = {k: v.decode("utf-8") for k, v in event_headers}
                
                context = trace_extract(headers)
                
                with tracer.start_as_current_span(
                    name="kafka.consume",
                    attributes={"kafka.message.topic": consume_topic},
                    context=context
                ) as span:

                    notification = json.loads(event.value().decode('utf-8'))

                    span.set_attributes({
                        "kafka.message.key": event.key().decode('utf-8'),
                        "kafka.message.value": json.dumps(notification)
                    })

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