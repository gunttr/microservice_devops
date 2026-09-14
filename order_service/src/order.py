from pydantic import BaseModel
from enum import Enum

class OrderCreate(BaseModel):
    username: str
    product_name: str
    quantity: int


class OrderStatus(str, Enum):
    CREATED = "created"
    PAYMENT_SUCCESS = "payment_success"
    PAYMENT_FAILED = "payment_failed"