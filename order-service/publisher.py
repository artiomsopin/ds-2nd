import json

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractExchange

EXCHANGE_NAME = "events"

_connection: AbstractRobustConnection | None = None
_exchange: AbstractExchange | None = None


async def connect(rabbitmq_url: str) -> None:
    global _connection, _exchange
    _connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await _connection.channel()
    _exchange = await channel.declare_exchange(
        EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
    )


async def disconnect() -> None:
    if _connection and not _connection.is_closed:
        await _connection.close()


async def publish(routing_key: str, payload: dict) -> None:
    await _exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )
