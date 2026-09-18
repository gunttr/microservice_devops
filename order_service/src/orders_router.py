from fastapi import HTTPException
from src.order import OrderCreate
from src.sql import pgsql_manager
from src.kafka import kafka_manager
from src.otel.logger import logger
from src.otel.metrics import orders_created_total
from fastapi import APIRouter
orders_router = APIRouter()


@orders_router.post("/orders")
async def orders(order: OrderCreate) -> dict[str, int]:
    order_id, created_at = pgsql_manager.create_order(
        username=order.username,
        product_name=order.product_name,
        quantity=order.quantity
    )

    logger.info(
        msg="Order successfully created",
        extra={
            "order_id": order_id,
            "username": order.username,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "created_at": str(created_at)
        }
    )

    orders_created_total.add(1)

    kafka_manager.produce_orders(
        order_id=order_id
    )

    kafka_manager.produce_notifications(
        order_id=order_id,
        order_username=order.username
    )
    
    return {
        "order_id": order_id
    }


@orders_router.get("/orders/{order_id}")
async def get_order(order_id: int):
    result = pgsql_manager.get_order(order_id=order_id)
              
    if not result:
        logger.warning(
            msg="Order not found",
            extra={
                "order_id": order_id
            }
        )

        raise HTTPException(
            status_code=404,
            detail="Order not found",
        )

    logger.info(
        msg="Order successfully retrieved",
        extra={
            "order_id": order_id
        }
    )

    return {
        "order_id": order_id,
        "status": result[0],
        "created_at": result[1]
    }
