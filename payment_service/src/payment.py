from enum import Enum
from src.logger import logger
from datetime import datetime
from random import random
import psycopg2 as pg
from fastapi import HTTPException
import os


class PaymentStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


postgres_user = os.getenv("POSTGRES_USER", "postgres_user")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
postgres_db = os.getenv("POSTGRES_DB", "postgres_db")
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")


def process_payments(order_id: int) -> PaymentStatus:
    status: PaymentStatus = PaymentStatus.SUCCESS
    created_at: datetime = datetime.now()

    if random() < 0.1: # типо ошибка внешнего апи
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
        logger.critical(f"Connection failed: {op_err}")

        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )

    except pg.Error as err:
        logger.error(f"Transaction failed: {err}")
        
        raise HTTPException(
            status_code=500,
            detail="Failed to process payment due to transaction issues"
        )

    if status == PaymentStatus.FAILED:
        logger.error(
            f"External payment API failed: "
            f"payment_id: {payment_id}; "
            f"order_id: {order_id}; "
            f"created_at: {created_at}"
        )
    else:
        logger.info(
            f"Payment was processed successfully: "
            f"payment_id: {payment_id}; "
            f"order_id: {order_id}; "
            f"created_at: {created_at}"
        )

    return status