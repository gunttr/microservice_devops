from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2 as pg
import os
from fastapi import HTTPException
import logging
from enum import Enum
import random

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


class OrderStatus(str, Enum):
    CREATED = "created"
    PAYMENT_SUCCESS = "payment_success"
    PAYMENT_FAILED = "payment_failed"


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
async def payments(order: OrderPay) -> dict[str, OrderStatus | str]:
    status: OrderStatus = OrderStatus.PAYMENT_SUCCESS

    if random.random() < 0.1: # типо ошибка внешнего апи
        status = OrderStatus.PAYMENT_FAILED
        logging.error(f"Payment API error: order_id is {order.id}")

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
                        UPDATE orders \
                        SET status = %s \
                        WHERE id = %s AND status = 'created' \
                        RETURNING id;
                        """, (status, order.id)
                    )

                    updated_order = cur.fetchone()
                    if not updated_order:
                        logging.warning(
                            f"Order {order.id} was already processed or does not exist"
                        )
                        return {
                            "status": "Already processed or does not exist"
                        }
                        
                
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
            detail="Failed to pay order"
        )

    logging.info(
        f"Order {order.id} was successfully paid"
    )

    return {
        "status": status
    }