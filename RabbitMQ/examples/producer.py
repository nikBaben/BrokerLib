"""Отправка одного события: python -m examples.producer."""

import asyncio

from aio_pika import ExchangeType

from examples.events import OrderCreated
from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.publisher import RabbitMQPublisher
from rabbitmq.topology import declare_exchange


async def main() -> None:
    settings = RabbitMQSettings()

    async with RabbitMQConnection(settings) as rabbitmq:
        channel = await rabbitmq.create_channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        exchange = await declare_exchange(
            channel,
            "orders.events",
            exchange_type=ExchangeType.TOPIC,
            durable=True,
        )

        publisher = RabbitMQPublisher(exchange)

        event = OrderCreated(
            order_id=123,
            user_id=456,
        )

        await publisher.publish(
            event,
            routing_key="order.created",
            headers={"source": "orders-service"},
            correlation_id=str(event.event_id),
            app_id="brokerlib-example",
            timeout=5.0,
        )

        print(f"Событие отправлено: {event.event_id}")


if __name__ == "__main__":
    asyncio.run(main())
