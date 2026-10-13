"""Проверки с локальным брокером при BROKERLIB_INTEGRATION=1.

Используются временные очереди и exchanges; прикладные очереди не читаются.
"""

import asyncio
import os
import unittest
from uuid import uuid4

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.exceptions import DeliveryError

from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.publisher import RabbitMQPublisher
from rabbitmq.schemas import Event
from rabbitmq.topology import bind_queue, declare_exchange, declare_queue


class SampleEvent(Event):
    value: int


@unittest.skipUnless(
    os.getenv("BROKERLIB_INTEGRATION") == "1",
    "Для проверки с брокером укажите BROKERLIB_INTEGRATION=1",
)
class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.connection = RabbitMQConnection(
            RabbitMQSettings(), name="brokerlib-integration", connect_timeout=5
        )
        await self.connection.connect()
        self.addAsyncCleanup(self.connection.close)
        self.channel = await self.connection.create_channel(on_return_raises=True)
        self.exchange = await declare_exchange(
            self.channel,
            f"brokerlib.test.{uuid4().hex}",
            durable=False,
            auto_delete=True,
            timeout=5,
        )
        self.addAsyncCleanup(self.exchange.delete, if_unused=False)

    async def test_properties_confirmation_and_manual_ack(self) -> None:
        queue = await declare_queue(
            self.channel,
            durable=False,
            exclusive=True,
            auto_delete=False,
            message_ttl=60000,
            max_priority=5,
            timeout=5,
        )
        await bind_queue(queue, self.exchange, routing_key="sample", timeout=5)
        received_messages: asyncio.Queue[
            tuple[SampleEvent, AbstractIncomingMessage]
        ] = asyncio.Queue()
        event = SampleEvent(value=42)

        async def handle(
            received: SampleEvent, message: AbstractIncomingMessage
        ) -> None:
            await message.ack()
            await received_messages.put((received, message))

        consumer = RabbitMQConsumer(
            self.channel, queue, SampleEvent, message_handler=handle, auto_ack=False
        )
        await consumer.start(prefetch_count=1, priority=2, timeout=5)
        self.addAsyncCleanup(consumer.stop)
        await RabbitMQPublisher(self.exchange).publish(
            event,
            routing_key="sample",
            headers={"trace": "integration"},
            correlation_id="request-42",
            reply_to="reply-queue",
            priority=3,
            expiration=30,
            message_type="sample.v1",
            app_id="test-app",
            delivery_mode=DeliveryMode.NOT_PERSISTENT,
            timeout=5,
        )
        received, message = await asyncio.wait_for(received_messages.get(), 5)
        self.assertEqual(received, event)
        self.assertEqual(message.headers, {"trace": "integration"})
        self.assertEqual(message.correlation_id, "request-42")
        self.assertEqual(message.reply_to, "reply-queue")
        self.assertEqual(message.priority, 3)
        self.assertEqual(message.app_id, "test-app")
        self.assertEqual(message.type, "sample.v1")
        self.assertEqual(message.expiration, 30.0)
        await consumer.stop()
        self.assertIsNone(await queue.get(fail=False, timeout=5))

    async def test_invalid_event_is_dead_lettered(self) -> None:
        dead = await declare_queue(
            self.channel, durable=False, exclusive=True, auto_delete=True
        )
        source = await declare_queue(
            self.channel,
            durable=False,
            exclusive=True,
            auto_delete=True,
            dead_letter_exchange="",
            dead_letter_routing_key=dead.name,
        )
        await bind_queue(source, self.exchange, routing_key="invalid")
        invoked = False

        async def handler(event):
            nonlocal invoked
            invoked = True

        consumer = RabbitMQConsumer(self.channel, source, SampleEvent, handler)
        await consumer.start()
        self.addAsyncCleanup(consumer.stop)
        with self.assertLogs("rabbitmq.consumer", level="ERROR"):
            await RabbitMQPublisher(self.exchange).publish_message(
                Message(b"not-json"), routing_key="invalid", timeout=5
            )
            async with asyncio.timeout(5):
                while True:
                    message = await dead.get(fail=False, timeout=1)
                    if message is not None:
                        break
                    await asyncio.sleep(0.02)
        self.assertEqual(message.body, b"not-json")
        self.assertIn("x-death", message.headers)
        await message.ack()
        self.assertFalse(invoked)

    async def test_mandatory_unroutable_message_fails(self) -> None:
        with self.assertRaises(DeliveryError):
            await RabbitMQPublisher(self.exchange).publish(
                SampleEvent(value=1), routing_key="unbound", timeout=5
            )


if __name__ == "__main__":
    unittest.main()
