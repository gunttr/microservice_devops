from fastapi import FastAPI
from pydantic import BaseModel
import os
import psycopg2 as pg
from enum import Enum
from datetime import datetime
from fastapi import HTTPException
import logging
from confluent_kafka import Producer, Consumer
from contextlib import asynccontextmanager
from threading import Thread
import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=consume_payments,
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
    "group.id": "order_service"
})
kafka_producer = Producer({
    "bootstrap.servers": "kafka:9092",
})

class OrderCreate(BaseModel):
    username: str
    product_name: str
    quantity: int


class OrderStatus(str, Enum):
    CREATED = "created"
    PAYMENT_SUCCESS = "payment_success"
    PAYMENT_FAILED = "payment_failed"


def execute_sql_querry(err_msg: str, query: str, args, fetch=False):
    try:
        with pg.connect(
                    dbname=postgres_db,
                    user=postgres_user,
                    password=postgres_password,
                    host=postgres_host,
                    port=postgres_port
                ) as con:
                    with con.cursor() as cur:
                        cur.execute(query, args)

                        if fetch:
                            return cur.fetchone()

                        return None

    except pg.OperationalError as op_err:
        logging.critical(f"Connection failed: {op_err}")

        raise HTTPException(
            status_code=500,
            detail=err_msg
        )

    except pg.Error as err:
        logging.error(f"Transaction failed: {err}")
        
        raise HTTPException(
            status_code=500,
            detail=err_msg
        )


def kafka_delivery_callback(err, msg):
    if err:
        logging.error(
            f"Event failed delivery: {err}"
        )
    else:
        logging.info(
            f"Produced event to topic {msg.topic()}: "
            f"key = {msg.key().decode('utf-8')}; "
            f"value = {msg.value().decode('utf-8')}"
        )


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is order service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }


@app.post("/orders")
async def orders(order: OrderCreate) -> dict[str, int | None]:
    created_at: datetime = datetime.now()

    result = execute_sql_querry(
        err_msg="Failed to create order",
        query=
        """
        INSERT INTO orders ( \
        username, \
        product_name,\
        quantity, \
        status, \
        created_at) \
        VALUES (%s, %s, %s, %s, %s ) \
        RETURNING id;
        """,
        args=
        (
        order.username,
        order.product_name,
        order.quantity,
        OrderStatus.CREATED,
        created_at
        ),
        fetch=True
    )
    order_id = result[0]

    logging.info(
    f"Order successfully created: "
    f"order_id: {order_id}; "
    f"username: {order.username}; "
    f"product_name: {order.product_name}; "
    f"quantity: {order.quantity}; "
    f"created_at: {created_at}"
    )

    # отправляем payment_service
    kafka_producer.produce(
        topic="orders",
        key="order_id",
        value=str(order_id),
        callback=kafka_delivery_callback
    )

    # отправляем notification_service
    kafka_producer.produce(
        topic="notifications",
        key="order",
        value=json.dumps(
            {
                "order_id": order_id,
                "username": order.username
            }
        ),
        callback=kafka_delivery_callback
    )

    kafka_producer.flush()
    
    return {
        "order_id": order_id
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    result = execute_sql_querry(
        err_msg="Failed to get order information",
        query=
        """
        SELECT \
        status, \
        created_at \
        FROM orders \
        WHERE id = %s;
        """, 
        args=(order_id, ),
        fetch=True
    )
              
    if not result:
        logging.warning(f"Order not found: id is {order_id}")

        raise HTTPException(
            status_code=404,
            detail="Order not found",
        )

    logging.info(f"Order successfully retrieved: id is {order_id} ")

    return {
        "order_id": order_id,
        "status": result[0],
        "created_at": result[1]
    }


def consume_payments():
    kafka_consumer.subscribe(["payments"])
    
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

            payment = json.loads(event.value().decode('utf-8'))
            update_order_status(payment=payment)
    except:
        logging.exception("Payment consumer crashed")
    finally:
        kafka_consumer.close()

def update_order_status(payment: dict[str, str]):
    order_id = int(payment.get("order_id"))

    if payment.get("status") == 'success':
        order_status = OrderStatus.PAYMENT_SUCCESS
    else:
        order_status = OrderStatus.PAYMENT_FAILED

    execute_sql_querry(
        err_msg="Failed to update order status",
        query=
        """
        UPDATE orders \
        SET status = %s \
        WHERE id = %s;
        """, 
        args=(order_status, order_id)
    )

    logging.info(
        f"Order status updated: "
        f"order_id: {order_id}; "
        f"status: {payment.get("status")}"
    )
