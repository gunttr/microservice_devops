from fastapi import FastAPI
from contextlib import asynccontextmanager
from threading import Thread
from src.kafka import kafka_manager
from src.otel.logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=kafka_manager.consume_notifications,
        daemon=True
    )
    thread.start()

    yield

    kafka_manager.close_consumer()

app = FastAPI(lifespan=lifespan)
logger.info("The notification service has just started")

@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is notification service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }
