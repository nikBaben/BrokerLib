# messaging/rabbitmq/topology.py

from typing import Any

from aio_pika import ExchangeType

from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
)


async def declare_exchange(
    channel: AbstractRobustChannel,
    name: str,
    *,
    exchange_type: ExchangeType = ExchangeType.TOPIC,
    durable: bool = True,
    **options: Any,
) -> AbstractRobustExchange:
    return await channel.declare_exchange(
        name=name,
        type=exchange_type,
        durable=durable,
        **options,
    )


async def declare_queue(
    channel: AbstractRobustChannel,
    name: str,
    *,
    durable: bool = True,
    arguments: dict[str, Any] | None = None,
    **options: Any,
) -> AbstractRobustQueue:
    return await channel.declare_queue(
        name=name,
        durable=durable,
        arguments=arguments,
        **options,
    )


async def bind_queue(
    queue: AbstractRobustQueue,
    exchange: AbstractRobustExchange,
    *,
    routing_key: str,
    **options: Any,
) -> None:
    await queue.bind(
        exchange,
        routing_key=routing_key,
        **options,
    )