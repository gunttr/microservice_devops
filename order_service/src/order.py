from enum import Enum

from pydantic import BaseModel


class OrderCreate(BaseModel):
    username: str
    product_name: str
    quantity: int


class OrderStatus(str, Enum):
    CREATED = "created"
    PAYMENT_SUCCESS = "payment_success"
    PAYMENT_FAILED = "payment_failed"