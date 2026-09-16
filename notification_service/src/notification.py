from random import random
from src.logger import logger
from src.metrics import (
    notifications_sent_total,
    notifications_failed_total,
    notifications_succeeded_total,
)

def send_notification(notification: dict[str, str]):
    order_id: int = int(notification.get("order_id"))
    username: str = notification.get("username")

    # обшее число отправленных сообщений +1
    notifications_sent_total.add(1)

    # настоящей отправки сообщений не будет
    if random() < 0.1: # имитируем ошибку отправки
        logger.error(
            f"Failed to send notification: "
            f"order_id: {order_id}; "
            f"username: {username}" 
        )
        notifications_failed_total.add(1)
        return
    
    logger.info(
        f"Successfully sent notification: "
        f"order_id: {order_id}; "
        f"username: {username}" 
    )
    notifications_succeeded_total.add(1)
