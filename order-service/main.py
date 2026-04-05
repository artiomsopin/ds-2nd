import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import publisher
from models import Base, Order

DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


# Schemas

class OrderItem(BaseModel):
    name: str
    quantity: int
    price: float


class OrderCreate(BaseModel):
    customer_name: str
    items: list[OrderItem]


class OrderResponse(BaseModel):
    id: uuid.UUID
    customer_name: str
    items: list[dict]
    total_amount: float
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


# --- DB dependency ---

async def get_db():
    async with SessionLocal() as session:
        yield session


# --- App ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await publisher.connect(RABBITMQ_URL)
    yield
    await publisher.disconnect()


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(data: OrderCreate, db: AsyncSession = Depends(get_db)):
    total = round(sum(i.quantity * i.price for i in data.items), 2)
    order = Order(
        id=uuid.uuid4(),
        customer_name=data.customer_name,
        items=[i.model_dump() for i in data.items],
        total_amount=total,
        status="pending",
        created_at=datetime.utcnow(),
    )
    db.add(order)
    await db.commit()
    await db.refresh(order)

    await publisher.publish("order.created", {
        "order_id": str(order.id),
        "customer_name": order.customer_name,           
        "total_amount": order.total_amount,
        "items": order.items,
    })
    return order


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).where(Order.id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
