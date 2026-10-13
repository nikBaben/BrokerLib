"""Обработка заказов из очереди: python -m examples.worker."""

import asyncio
import logging

from aio_pika import ExchangeType
from aio_pika.abc import AbstractIncomingMessage

from examples.events import OrderCreated
from examples.names import Exchanges, Queues, RoutingKeys
from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.topology import bind_queue, declare_exchange, declare_queue

logging.basicConfig(level=logging.INFO)


async def handle_order_created(
    event: OrderCreated,
    message: AbstractIncomingMessage,
) -> None:
    print(f"Заказ: {event.event_id}")
    await asyncio.sleep(10)

    await message.ack()
    print(f"Заказ обработан: {event.event_id}")


async def main() -> None:
    settings = RabbitMQSettings()

    async with RabbitMQConnection(settings) as rabbitmq:
        channel = await rabbitmq.create_channel(
            publisher_confirms=False,
        )

        exchange = await declare_exchange(
            channel,
            Exchanges.ORDERS,
            exchange_type=ExchangeType.TOPIC,
            durable=True,
        )

        queue = await declare_queue(
            channel,
            Queues.PAYMENTS_ORDERS,
            durable=True,
            max_length=5,
            queue_type="quorum",
            overflow="reject-publish",
        )

        await bind_queue(
            queue,
            exchange,
            routing_key=RoutingKeys.ORDER_CREATED,
        )

        consumer = RabbitMQConsumer(
            channel=channel,
            queue=queue,
            event_type=OrderCreated,
            auto_ack=False,
            message_handler=handle_order_created,
        )

        await consumer.start(
            prefetch_count=1,
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
