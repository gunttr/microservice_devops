from fastapi import FastAPI
from contextlib import asynccontextmanager
from threading import Thread
from src.kafka import kafka_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(
        target=kafka_manager.consume_orders,
        daemon=True
    )
    thread.start()

    yield

    kafka_manager.consumer_close()


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root() -> dict[str, str]:
    return {
        "message": "Hello, this is payment service!"
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "healthy"
    }
