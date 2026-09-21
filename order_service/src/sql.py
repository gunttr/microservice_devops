import os
from datetime import datetime

import psycopg2 as pg
from fastapi import HTTPException
from src.order import OrderStatus
from src.otel.logger import logger
from src.otel.metrics import payment_failed_orders_total, payment_success_orders_total
from src.otel.traces import trace, tracer


class PGSQLManager:
    def __init__(self, user: str, password: str, db: str, host: str, port: str) -> None:
        self.__user = user
        self.__password = password
        self.__db = db
        self.__host = host
        self.__port = port


    def __execute_sql_querry(self, err_msg: str, query: str, args, fetch=False):
        with tracer.start_as_current_span(
            name="db.execute_querry",
            attributes={
                "db.dbms": "postgresql",
                "db.query": query
            }
        ) as span:
            try:
                with pg.connect(
                            dbname=self.__db,
                            user=self.__user,
                            password=self.__password,
                            host=self.__host,
                            port=self.__port
                        ) as con, con.cursor() as cur:
                    cur.execute(query, args)

                    if fetch:
                        span.set_attribute("db.result.fetched", True)
                        return cur.fetchone()

                    span.set_attribute("db.result.fetched", False)
                    return None

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
                    detail=err_msg
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
                    detail=err_msg
                )


    def create_order(self, username: str, product_name: str, quantity: int) -> tuple[int, datetime]:
        with tracer.start_as_current_span(
            name="business.create_order",
            attributes={
                "order.username": username,
                "order.product_name": product_name,
                "order.quantity": quantity
            }
        ) as span:
            created_at: datetime = datetime.now()
            
            result = self.__execute_sql_querry(
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
                username,
                product_name,
                quantity,
                OrderStatus.CREATED,
                created_at
                ),
                fetch=True
            )
            order_id = result[0]

            span.set_attributes({
                "order.id": order_id,
                "order.created_at": str(created_at)
            })
            
            return order_id, created_at


    def get_order(self, order_id: int):
        with tracer.start_as_current_span(
            "business.get_order",
            attributes={"order.id": order_id}
        ):
            result = self.__execute_sql_querry(
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

            return result


    def update_order_status(self, payment: dict[str, str]):
        order_id: int = int(payment.get("order_id", 0))

        with tracer.start_as_current_span(
            name="business.update_order_status",
            attributes={
                "order.id": order_id,
            }
        ) as span:
            if payment.get("status") == 'success':
                order_status = OrderStatus.PAYMENT_SUCCESS
                payment_success_orders_total.add(1)
                span.set_attribute("order.status", "payment_success")
            else:
                order_status = OrderStatus.PAYMENT_FAILED
                payment_failed_orders_total.add(1)
                span.set_attribute("order.status", "payment_failed")

            self.__execute_sql_querry(
                err_msg="Failed to update order status",
                query=
                """
                UPDATE orders \
                SET status = %s \
                WHERE id = %s;
                """, 
                args=(order_status, order_id)
            )
    
        logger.info(
            msg="Order status updated",
            extra={
                "order_id": order_id,
                "status": payment.get("status")
            }
        )


pgsql_manager: PGSQLManager = PGSQLManager(
    user=os.getenv("POSTGRES_USER", "postgres_user"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres_password"),
    db=os.getenv("POSTGRES_DB", "postgres_db"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432")
)
