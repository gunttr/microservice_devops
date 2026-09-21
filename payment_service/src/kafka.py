import json
import os

from confluent_kafka import Consumer, Producer
from opentelemetry.propagate import extract as trace_extract
from opentelemetry.propagate import inject as trace_inject
from src.otel.logger import logger
from src.otel.traces import tracer
from src.payment import PaymentStatus, process_payments


class KafkaHeaderSetter:
    def set(self, carrier, key, value):
        # carrier - это список, добавляем кортеж (key, value_bytes)
        if isinstance(value, str):
            value = value.encode('utf-8')
        carrier.append((key, value))

kafka_setter_instance = KafkaHeaderSetter()

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


    def consume_orders(self):
        consume_topic: str = "orders"
        self.__kafka_consumer.subscribe([consume_topic])

        try:
            while True:
                event = self.__kafka_consumer.poll(1.0)

                if not event:
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
                
                event_value: str = event.value().decode('utf-8')

                with tracer.start_as_current_span(
                    name="kafka.consume",
                    attributes={
                        "kafka.message.topic": consume_topic,
                        "kafka.message.key": event.key().decode('utf-8'),
                        "kafka.message.value": event_value,
                    },
                    context=context
                ):
                
                    order_id: int = int(event_value)
                    status: PaymentStatus = process_payments(order_id=order_id)
                    self.__produce_payments(order_id, status)

        except Exception as err:
            logger.error(
                msg="Kafka consumer crashed",
                extra={
                    "topic": consume_topic,
                    "error": str(err)
                }
            )
        finally:
            self.__kafka_consumer.close()


    def __produce_payments(self, order_id: int, status: PaymentStatus):
        topic = "payments"
        key = "payment"
        value = json.dumps({
            "order_id": order_id,
            "status": status.value
        }).encode('utf-8')

        headers = []
        trace_inject(headers, setter=kafka_setter_instance)

        with tracer.start_as_current_span(
            name="kafka.produce",
            attributes={
                "kafka.message.topic": topic,
                "kafka.message.key": key,
                "kafka.message.value": value
            }
        ):
            self.__kafka_producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                callback=self.__kafka_delivery_callback
            )
            self.__kafka_producer.flush()

    def consumer_close(self):
        self.__kafka_consumer.close()


kafka_manager: KafkaManager = KafkaManager(
    hostname=os.getenv("KAFKA_HOSTNAME", "localhost"),
    port=os.getenv("KAFKA_PORT", "9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "payment_service")
)