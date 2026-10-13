"""Обработка заказов из очереди: python -m examples.worker."""

import asyncio
import logging

from aio_pika import ExchangeType

from examples.events import OrderCreated
from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.topology import bind_queue, declare_exchange, declare_queue

logging.basicConfig(level=logging.INFO)


async def handle_order_created(
    event: OrderCreated,
) -> None:
    print(f"Обработка заказа: {event.order_id}")

    await asyncio.sleep(1)

    print(f"Заказ обработан: {event.event_id}")


async def main() -> None:
    settings = RabbitMQSettings()

    async with RabbitMQConnection(settings) as rabbitmq:
        channel = await rabbitmq.create_channel(
            publisher_confirms=False,
        )

        exchange = await declare_exchange(
            channel,
            "orders.events",
            exchange_type=ExchangeType.TOPIC,
            durable=True,
        )

        queue = await declare_queue(
            channel,
            "payments.orders",
            durable=True,
            queue_type="quorum",
        )

        await bind_queue(
            queue,
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

        print("Обработчик запущен. Ожидание сообщений...")

        try:
            await asyncio.Event().wait()
        finally:
            await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
