from fastapi import FastAPI
from pydantic import BaseModel
import os
import psycopg2 as pg
from enum import Enum
from datetime import datetime
from fastapi import HTTPException
import logging

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


class OrderCreate(BaseModel):
    username: str
    product_name: str
    quantity: int


class OrderStatus(str, Enum):
    CREATED = "created"
    PAYMENT_SUCCESS = "payment_success"
    PAYMENT_FAILED = "payment_failed"


async def execute_sql_querry(err_msg: str, query: str, args):
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

                        return cur.fetchone()

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

    result = await execute_sql_querry(
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
        )
    )

    logging.info(
    f"Order successfully created: "
    f"order_id: {result[0]}; "
    f"username: {order.username}; "
    f"product_name: {order.product_name}; "
    f"quantity: {order.quantity}"
    f"created_at: {created_at}"
    )
    
    return {
        "order_id": result[0]
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    result = await execute_sql_querry(
        err_msg="Failed to get order information",
        query=
        """
        SELECT \
        status, \
        created_at \
        FROM orders \
        WHERE id = %s;
        """, 
        args=(order_id, )
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
    