import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import consumer
from models import Base, Notification

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5435/notifications")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db():
    async with SessionLocal() as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await consumer.start(RABBITMQ_URL, DATABASE_URL)
    yield
    await consumer.stop()


app = FastAPI(title="Notification Service", lifespan=lifespan)


@app.get("/notifications/{order_id}")
async def get_notifications(order_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Notification).where(Notification.order_id == order_id)
    )
    notifications = result.scalars().all()
    if not notifications:
        raise HTTPException(status_code=404, detail="No notifications found for this order")
    return notifications


@app.get("/health")
async def health():
    return {"status": "ok", "service": "notification-service"}
