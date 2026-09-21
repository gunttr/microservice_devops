from enum import Enum
from src.otel.logger import logger
from datetime import datetime
from random import random
import psycopg2 as pg
from fastapi import HTTPException
import os
from src.otel.metrics import (
    payments_processed_total,
    payments_succeeded_total,
    payments_failed_total
)
from src.otel.traces import tracer, trace

class PaymentStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


postgres_user = os.getenv("POSTGRES_USER", "postgres_user")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
postgres_db = os.getenv("POSTGRES_DB", "postgres_db")
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")


def execute_sql_querry(query: str, args) -> int:
    with tracer.start_as_current_span(
        name="db.execute_querry",
        attributes={
            "db.dbms": "postgresql",
            "db.query": query
        }
    ) as span:
        try:
            with pg.connect(
                dbname=postgres_db,
                user=postgres_user,
                password=postgres_password,
                host=postgres_host,
                port=postgres_port) as con:
                    with con.cursor() as cur:
                        cur.execute(query, args)

                        return cur.fetchone()[0]   


        except pg.OperationalError as op_err:
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            span.record_exception(op_err)
            logger.critical(
                msg="Connection to PostgreSQL database failed",
                extra={
                    "error": str(op_err),
                }
            )

            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )

        except pg.Error as err:
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            span.record_exception(err)
            logger.error(
                msg="PostgreSQL transaction failed",
                extra={
                    "error": str(err)
                }
            )
            
            raise HTTPException(
                status_code=500,
                detail="Failed to process payment due to database transaction issues"
            )


def process_payments(order_id: int) -> PaymentStatus:
    created_at: datetime = datetime.now()

    with tracer.start_as_current_span(
        name="business.process_payment",
        attributes={
            "payment.created_at": str(created_at)
        }
    ) as span:
        
        status: PaymentStatus = PaymentStatus.SUCCESS

        if random() < 0.1: # типо ошибка внешнего апи
            status = PaymentStatus.FAILED

        payment_id: int = execute_sql_querry(
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

        span.set_attributes({
            "payment.id": payment_id,
            "payment.status": status
        })


    if status == PaymentStatus.FAILED:
        logger.error(
            msg="External payment API failed",
            extra={
                "payment_id": payment_id,
                "order_id": order_id,
                "created_at": str(created_at)
            }
            
        )
        payments_failed_total.add(1)
    else:
        logger.info(
            msg="Payment was processed successfully",
            extra={
                "payment_id": payment_id,
                "order_id": order_id,
                "created_at": str(created_at)
            }
        )
        payments_succeeded_total.add(1)

    payments_processed_total.add(1)

    return status