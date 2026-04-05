import json
import uuid
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

EXCHANGE_NAME = "events"
QUEUE_NAME = "notification_queue"

_MESSAGES = {
    "payment.completed": "Dear {name}, your payment of ${amount:.2f} for order {order_id} was SUCCESSFUL.",
    "payment.failed":    "Dear {name}, your payment of ${amount:.2f} for order {order_id} FAILED. Please retry.",
}

_connection: AbstractRobustConnection | None = None


async def start(rabbitmq_url: str, database_url: str) -> None:
    global _connection

    engine = create_async_engine(database_url)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async def handle(message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            data = json.loads(message.body)
            order_id = data["order_id"]
            customer_name = data.get("customer_name", "Customer")
            amount = data.get("amount", 0.0)
            event_type = message.routing_key or "unknown"

            text = _MESSAGES.get(event_type, "Payment event received.").format(
                name=customer_name, amount=amount, order_id=order_id
            )

            from models import Notification
            async with SessionLocal() as db:
                notification = Notification(
                    id=uuid.uuid4(),
                    order_id=uuid.UUID(order_id),
                    event_type=event_type,
                    customer_name=customer_name,
                    message=text,
                    created_at=datetime.utcnow(),
                )
                db.add(notification)
                await db.commit()

            # Simulate email / SMS
            print(f"[notification] EMAIL → {text}", flush=True)

    _connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await _connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key="payment.completed")
    await queue.bind(exchange, routing_key="payment.failed")
    await queue.consume(handle)
    print("[notification] consumer ready — listening for payment.completed / payment.failed", flush=True)


async def stop() -> None:
    if _connection and not _connection.is_closed:
        await _connection.close()
