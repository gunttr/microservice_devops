from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2 as pg
import os
from fastapi import HTTPException
import logging
from enum import Enum
import random
from datetime import datetime
from confluent_kafka import Consumer, Producer
from contextlib import asynccontextmanager
from threading import Thread
import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=consume_orders,
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

postgres_user = os.getenv("POSTGRES_USER", "postgres_user")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
postgres_db = os.getenv("POSTGRES_DB", "postgres_db")
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")


kafka_consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "payment_service"
})
kafka_producer = Producer({
    "bootstrap.servers": "kafka:9092",
})


class PaymentStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


def kafka_delivery_callback(err, msg):
    if err:
        logging.error(
            f"Event failed delivery: {err}"
        )
    else:
        logging.info(
            f"Produced event to topic payments: "
            f"key = {msg.key().decode('utf-8')}; "
            f"value = {msg.value().decode('utf-8')}"
        )


def consume_orders():
    kafka_consumer.subscribe(["orders"])

    try:
        while True:
            event = kafka_consumer.poll(1.0)

            if not event:
                continue

            if event.error():
                logging.error(
                    f"Kafka consumer error: {event.error()}"
                )
                continue
    
            order_id: int = int(event.value().decode('utf-8'))
            status: PaymentStatus = process_payments(order_id=order_id)
            kafka_producer.produce(
                topic="payments",
                key="payment",
                value=json.dumps({
                    "order_id": order_id,
                    "status": status.value
                }).encode('utf-8'),
                callback=kafka_delivery_callback
            )
            kafka_producer.flush()
    finally:
        kafka_consumer.close()


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is payment service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }


def process_payments(order_id: int) -> PaymentStatus:
    status: PaymentStatus = PaymentStatus.SUCCESS
    created_at: datetime = datetime.now()

    if random.random() < 0.1: # типо ошибка внешнего апи
        status = PaymentStatus.FAILED

    try:
        with pg.connect(
            dbname=postgres_db,
            user=postgres_user,
            password=postgres_password,
            host=postgres_host,
            port=postgres_port) as con:
                with con.cursor() as cur:
                    cur.execute(
                        """
                        INSERT into payments ( \
                        order_id, \
                        status, \
                        created_at) \
                        VALUES (%s, %s, %s) \
                        RETURNING id;
                        """, 
                        (order_id, status, created_at)
                    )

                    payment_id = cur.fetchone()[0]   
                
    except pg.OperationalError as op_err:
        logging.critical(f"Connection failed: {op_err}")

        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )

    except pg.Error as err:
        logging.error(f"Transaction failed: {err}")
        
        raise HTTPException(
            status_code=500,
            detail="Failed to process payment due to transaction issues"
        )

    if status == PaymentStatus.FAILED:
        logging.error(
            f"External payment API failed: "
            f"payment_id: {payment_id}; "
            f"order_id: {order_id}; "
            f"created_at: {created_at}"
        )
    else:
        logging.info(
            f"Payment was processed successfully: "
            f"payment_id: {payment_id}; "
            f"order_id: {order_id}; "
            f"created_at: {created_at}"
        )

    return status
