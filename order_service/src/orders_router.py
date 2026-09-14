from fastapi import HTTPException
from src.order import OrderCreate
from src.sql import pgsql_manager
from src.kafka import kafka_manager
from src.logger import logger
from fastapi import APIRouter
orders_router = APIRouter()


@orders_router.post("/orders")
async def orders(order: OrderCreate) -> dict[str, int | None]:
    order_id, created_at = pgsql_manager.create_order(
        username=order.username,
        product_name=order.product_name,
        quantity=order.quantity
    )

    logger.info(
    f"Order successfully created: "
    f"order_id: {order_id}; "
    f"username: {order.username}; "
    f"product_name: {order.product_name}; "
    f"quantity: {order.quantity}; "
    f"created_at: {created_at}"
    )

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
        logger.warning(f"Order not found: id is {order_id}")

        raise HTTPException(
            status_code=404,
            detail="Order not found",
        )

    logger.info(f"Order successfully retrieved: id is {order_id} ")

    return {
        "order_id": order_id,
        "status": result[0],
        "created_at": result[1]
    }
