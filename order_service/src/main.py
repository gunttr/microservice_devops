from fastapi import FastAPI
from contextlib import asynccontextmanager
from threading import Thread
from src.kafka import kafka_manager
from src.orders_router import orders_router
from src.otel.logger import logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=kafka_manager.consume_payments,
        daemon=True
    )
    thread.start()

    yield

    kafka_manager.close_consumer()


app = FastAPI(lifespan=lifespan)
app.include_router(orders_router)
logger.info("The order service has just started")

@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is order service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }
