from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2 as pg
import os
from fastapi import HTTPException
import logging
from enum import Enum
import random
from datetime import datetime

app = FastAPI()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", logging.INFO),
    format="%(levelname)s | %(asctime)s | %(message)s",
)

postgres_user = os.getenv("POSTGRES_USER", "postgres_user")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
postgres_db = os.getenv("POSTGRES_DB", "postgres_db")
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")

class OrderPay(BaseModel):
    id: int


class PaymentStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


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


@app.post("/payments")
async def payments(order: OrderPay) -> dict[str, PaymentStatus]:
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
                        (order.id, status, created_at)
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
            f"order_id: {order.id}; "
            f"created_at: {created_at}"
        )
    else:
        logging.info(
            f"Payment was processed successfully: "
            f"payment_id: {payment_id}; "
            f"order_id: {order.id}; "
            f"created_at: {created_at}"
        )

    return {
        "payment_status": status
    }