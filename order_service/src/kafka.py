import json
import os

from confluent_kafka import Consumer, Producer
from opentelemetry.propagate import extract as trace_extract
from opentelemetry.propagate import inject as trace_inject
from src.otel.logger import logger
from src.otel.traces import trace, tracer
from src.sql import pgsql_manager


class KafkaHeaderSetter:
    def set(self, carrier, key, value):
        # carrier - это список, добавляем кортеж (key, value_bytes)
        if isinstance(value, str):
            value = value.encode('utf-8')
        carrier.append((key, value))

kafka_setter_instance = KafkaHeaderSetter()

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

                headers = {}
                if event_headers := event.headers():
                    headers = {k: v.decode("utf-8") for k, v in event_headers}
                logger.info(f"{headers}")
                context = trace_extract(headers)
                
                with tracer.start_as_current_span(
                    name="kafka.consume",
                    attributes={"kafka.message.topic": consume_topic},
                    context=context
                ) as span:
                    payment = json.loads(event.value().decode('utf-8'))

                    span.set_attributes({
                        "messaging.message.order_id": payment.get("order_id"),
                        "messaging.message.status": payment.get("status")
                    })

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
        with tracer.start_as_current_span(
            name="kafka.produce_notifications",
            attributes={
                "kafka.message.topic": "notifications"
            }
        ) as span:
            produce_value = json.dumps({
                "order_id": order_id,
                "username": order_username
            })

            headers = []
            trace_inject(headers, setter=kafka_setter_instance)
            
            self.__kafka_producer.produce(
                topic="notifications",
                key="order",
                value=produce_value,
                headers=headers,
                callback=self.__kafka_delivery_callback
            )

            span.set_attributes({
                "kafka.message.key": "order",
                "kafka.message.value": produce_value
            })
    
        self.__kafka_producer.flush()

    # отправляем payment_service
    def produce_orders(self, order_id: int):
        with tracer.start_as_current_span(
            name="kafka.produce_orders",
            attributes={
                "kafka.message.topic": "orders"
            }
        ) as span:

            headers = []
            trace_inject(headers, setter=kafka_setter_instance)

            self.__kafka_producer.produce(
                topic="orders",
                key="order_id",
                value=str(order_id),
                headers=headers,
                callback=self.__kafka_delivery_callback
            )

            span.set_attributes({
                "kafka.message.key": "order_id",
                "kafka.message.value": str(order_id)
            })
            
            self.__kafka_producer.flush()


    def close_consumer(self):
        self.__kafka_consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "order_service")
)