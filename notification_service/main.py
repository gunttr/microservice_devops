from fastapi import FastAPI
import os
import logging
from contextlib import asynccontextmanager
from threading import Thread
from confluent_kafka import Consumer
from random import random
import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=consume_notifications,
        daemon=True
    )
    thread.start()

    yield

    kafka_consumer.close()

app = FastAPI(lifespan=lifespan)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", logging.INFO),
    format="%(levelname)s | %(asctime)s | %(message)s",
)

kafka_consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "notification_service"
})

@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is notification service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }

def consume_notifications():
    kafka_consumer.subscribe(["notifications"])
    
    try:
        while True:
            event = kafka_consumer.poll(1.0)
            
            if event is None:
                continue
                
            if event.error():
                logging.error(
                    f"Kafka consumer error: {event.error()}"
                )
                continue

            notification = json.loads(event.value().decode('utf-8'))
            send_notification(notification=notification)
        
    except:
        logging.exception("Notification consumer crashed")
    finally:
        kafka_consumer.close()


def send_notification(notification: dict[str, str]):
    order_id: int = int(notification.get("order_id"))
    username: str = notification.get("username")

    # настоящей отправки сообщений не будет
    if random() < 0.1: # имитируем ошибку отправки
        logging.error(
            f"Failed to send notification: "
            f"order_id: {order_id}; "
            f"username: {username}" 
        )
        return
    
    logging.info(
        f"Successfully sent notification: "
        f"order_id: {order_id}; "
        f"username: {username}" 
    )