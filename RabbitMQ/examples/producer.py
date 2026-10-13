# examples/producer.py

import asyncio

from aio_pika import ExchangeType

from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.publisher import RabbitMQPublisher

from examples.events import OrderCreated


async def main() -> None:
    settings = RabbitMQSettings()

    async with RabbitMQConnection(settings) as rabbitmq:
        channel = await rabbitmq.create_channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        exchange = await channel.declare_exchange(
            "orders.events",
            type=ExchangeType.TOPIC,
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
        )

        print(f"Event published: {event.event_id}")


if __name__ == "__main__":
    asyncio.run(main())