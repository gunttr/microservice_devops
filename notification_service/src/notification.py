from random import random

from src.otel.logger import logger
from src.otel.metrics import (
    notifications_failed_total,
    notifications_sent_total,
    notifications_succeeded_total,
)
from src.otel.traces import tracer


def send_notification(notification: dict[str, str]):
    order_id: int = int(notification.get("order_id"))
    username: str = notification.get("username")
    with tracer.start_as_current_span(
        name="business.send_notifications",
        attributes={
            "notification.order_id": order_id,
            "notification.username": username 
        }
    ) as span:
        
        # обшее число отправленных сообщений +1
        notifications_sent_total.add(1)

        # настоящей отправки сообщений не будет
        if random() < 0.1: # имитируем ошибку отправки
            logger.error(
                msg="Failed to send notification",
                extra={
                    "order_id": order_id,
                    "username": username
                }
            )
            span.set_attribute("notification.status", "failed")
            notifications_failed_total.add(1)
            return

        span.set_attribute("notification.status", "success")

    notifications_succeeded_total.add(1)
    
    logger.info(
        msg="Successfully sent notification",
        extra={
            "order_id": order_id,
            "username": username
        }
    )
