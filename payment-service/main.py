import json
import os
import uuid
from contextlib import asynccontextmanager

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractExchange
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import consumer
from models import Base, Payment

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5434/payments")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")

EXCHANGE_NAME = "events"
_pub_connection: AbstractRobustConnection | None = None
_exchange: AbstractExchange | None = None

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db():
    async with SessionLocal() as session:
        yield session


async def publish(routing_key: str, payload: dict) -> None:
    await _exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pub_connection, _exchange

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    _pub_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await _pub_connection.channel()
    _exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)

    await consumer.start(RABBITMQ_URL, DATABASE_URL, publish)
    yield
    await consumer.stop()
    if _pub_connection and not _pub_connection.is_closed:
        await _pub_connection.close()


app = FastAPI(title="Payment Service", lifespan=lifespan)


@app.get("/payments/{order_id}")
async def get_payment(order_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Payment).where(Payment.order_id == order_id))
    payment = result.scalar_one_or_none()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found for this order")
    return payment


@app.get("/health")
async def health():
    return {"status": "ok", "service": "payment-service"}
