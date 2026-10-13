# examples/worker.py

import asyncio
import logging

from aio_pika import ExchangeType

from messaging.rabbitmq.config import RabbitMQSettings
from messaging.rabbitmq.connection import RabbitMQConnection
from messaging.rabbitmq.consumer import RabbitMQConsumer

from examples.events import OrderCreated


logging.basicConfig(level=logging.INFO)


async def handle_order_created(
    event: OrderCreated,
) -> None:
    print(f"Processing order: {event.order_id}")

    await asyncio.sleep(1)

    print(f"Order processed: {event.event_id}")


async def main() -> None:
    settings = RabbitMQSettings()

    async with RabbitMQConnection(settings) as rabbitmq:
        channel = await rabbitmq.create_channel(
            publisher_confirms=False,
        )

        exchange = await channel.declare_exchange(
            "orders.events",
            type=ExchangeType.TOPIC,
            durable=True,
        )

        queue = await channel.declare_queue(
            "payments.orders",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
            },
        )

        await queue.bind(
            exchange,
            routing_key="order.created",
        )

        consumer = RabbitMQConsumer(
            channel=channel,
            queue=queue,
            event_type=OrderCreated,
            handler=handle_order_created,
        )

        await consumer.start(
            prefetch_count=settings.prefetch_count,
        )

        print("Worker started. Waiting for messages...")

        try:
            await asyncio.Event().wait()
        finally:
            await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass