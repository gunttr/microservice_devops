from random import random
from src.logger import logger


def send_notification(notification: dict[str, str]):
    order_id: int = int(notification.get("order_id"))
    username: str = notification.get("username")

    # настоящей отправки сообщений не будет
    if random() < 0.1: # имитируем ошибку отправки
        logger.error(
            f"Failed to send notification: "
            f"order_id: {order_id}; "
            f"username: {username}" 
        )
        return
    
    logger.info(
        f"Successfully sent notification: "
        f"order_id: {order_id}; "
        f"username: {username}" 
    )