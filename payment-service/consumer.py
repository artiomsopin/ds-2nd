import json
import random
import uuid
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

EXCHANGE_NAME = "events"
QUEUE_NAME = "payment_queue"

_connection: AbstractRobustConnection | None = None


async def start(rabbitmq_url: str, database_url: str, publish_fn) -> None:
    global _connection

    engine = create_async_engine(database_url)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async def handle(message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            data = json.loads(message.body)
            order_id = data["order_id"]
            amount = data["total_amount"]
            customer_name = data.get("customer_name", "")

            success = random.random() < 0.8
            status = "completed" if success else "failed"

            from models import Payment
            async with SessionLocal() as db:
                payment = Payment(
                    id=uuid.uuid4(),
                    order_id=uuid.UUID(order_id),
                    amount=amount,
                    status=status,
                    processed_at=datetime.utcnow(),
                )
                db.add(payment)
                await db.commit()

            routing_key = "payment.completed" if success else "payment.failed"
            await publish_fn(routing_key, {
                "order_id": order_id,
                "payment_id": str(payment.id),
                "amount": amount,
                "status": status,
                "customer_name": customer_name,
            })
            print(f"[payment] order={order_id} → {status}", flush=True)

    _connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await _connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key="order.created")
    await queue.consume(handle)
    print("[payment] consumer ready — listening for order.created", flush=True)


async def stop() -> None:
    if _connection and not _connection.is_closed:
        await _connection.close()
