"""Проверки поведения без брокера; для запуска достаточно unittest."""

import asyncio
import unittest
from unittest.mock import AsyncMock, create_autospec, patch

from aio_pika import DeliveryMode, Message
from aio_pika.abc import (
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
    AbstractRobustQueue,
)
from pydantic_settings import SettingsConfigDict

from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.publisher import RabbitMQPublisher
from rabbitmq.schemas import Event
from rabbitmq.topology import bind_exchange, bind_queue, declare_exchange, declare_queue


class TestSettings(RabbitMQSettings):
    """Настройки тестов не читают рабочий файл .env."""

    model_config = SettingsConfigDict(env_file=None)


class Order(Event):
    order_id: int


def incoming(body: bytes | None = None):
    # Мок повторяет интерфейс входящего сообщения, включая async ack/reject.
    message = create_autospec(AbstractIncomingMessage, instance=True)
    if body is None:
        body = Order(order_id=1).model_dump_json().encode()
    message.body = body
    message.processed = False
    return message


class ConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_overrides_and_zero_values(self) -> None:
        settings = TestSettings(url="amqp://original/")
        raw = create_autospec(AbstractRobustConnection, instance=True)
        raw.is_closed = False
        with patch(
            "rabbitmq.connection.aio_pika.connect_robust",
            new_callable=AsyncMock,
            return_value=raw,
        ) as connect:
            connection = RabbitMQConnection(
                settings,
                url=None,
                host="broker",
                port=5673,
                login="app",
                password="secret",
                virtualhost="test",
                heartbeat=0,
                fail_fast=False,
                connect_timeout=None,
                name="unit-test",
                interleave=1,
                happy_eyeballs_delay=0.25,
                client_properties={"connection_name": "unit-test"},
            )
            await asyncio.gather(connection.connect(), connection.connect())
            connect.assert_awaited_once()
            self.assertIsNone(connect.call_args.args[0])
            options = connect.call_args.kwargs
            for key, value in {
                "host": "broker",
                "port": 5673,
                "login": "app",
                "password": "secret",
                "virtualhost": "test",
                "heartbeat": 0,
                "fail_fast": False,
                "timeout": settings.connect_timeout,
                "name": "unit-test",
                "interleave": 1,
                "happy_eyeballs_delay": 0.25,
            }.items():
                self.assertEqual(options[key], value)
            await connection.close()
            raw.close.assert_awaited_once()
        self.assertEqual(settings.url, "amqp://original/")
        self.assertEqual(settings.heartbeat, 60)

    async def test_connection_timeout_from_settings_can_be_disabled(self) -> None:
        settings = TestSettings(connect_timeout=None)
        with patch(
            "rabbitmq.connection.aio_pika.connect_robust", new_callable=AsyncMock
        ) as connect:
            connection = RabbitMQConnection(settings)
            await connection.connect()
            self.assertIsNone(connect.call_args.kwargs["timeout"])
            self.assertEqual(connect.call_args.args[0], settings.url)

    async def test_channel_requires_connection(self) -> None:
        connection = RabbitMQConnection(TestSettings())
        with self.assertRaisesRegex(RuntimeError, "не установлено"):
            await connection.create_channel()


class TopologyTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_extensions_preserve_false_zero_and_empty_string(self) -> None:
        channel = create_autospec(AbstractRobustChannel, instance=True)
        await declare_queue(
            channel,
            "orders",
            durable=False,
            exclusive=True,
            auto_delete=True,
            robust=False,
            timeout=4,
            queue_type="classic",
            message_ttl=0,
            max_length=100,
            dead_letter_exchange="",
            single_active_consumer=False,
            arguments={"custom-extension": "value"},
        )
        options = channel.declare_queue.call_args.kwargs
        self.assertEqual(
            options["arguments"],
            {
                "custom-extension": "value",
                "x-queue-type": "classic",
                "x-message-ttl": 0,
                "x-max-length": 100,
                "x-dead-letter-exchange": "",
                "x-single-active-consumer": False,
            },
        )
        self.assertFalse(options["durable"])
        self.assertFalse(options["robust"])
        self.assertTrue(options["exclusive"])
        self.assertEqual(options["timeout"], 4)

    async def test_duplicate_extensions_fail_before_rpc(self) -> None:
        channel = create_autospec(AbstractRobustChannel, instance=True)
        with self.assertRaisesRegex(ValueError, "x-message-ttl"):
            await declare_queue(
                channel, "orders", message_ttl=5, arguments={"x-message-ttl": 10}
            )
        channel.declare_queue.assert_not_awaited()

    async def test_exchange_and_binding_options(self) -> None:
        channel = create_autospec(AbstractRobustChannel, instance=True)
        await declare_exchange(
            channel,
            "events",
            exchange_type="x-delayed-message",
            delayed_type="topic",
            alternate_exchange="fallback",
            internal=True,
            passive=True,
            robust=False,
            timeout=2,
        )
        options = channel.declare_exchange.call_args.kwargs
        self.assertEqual(
            options["arguments"],
            {"x-delayed-type": "topic", "alternate-exchange": "fallback"},
        )
        self.assertTrue(options["internal"])
        self.assertTrue(options["passive"])
        queue = create_autospec(AbstractRobustQueue, instance=True)
        await bind_queue(
            queue, "events", arguments={"x-match": "all"}, robust=False, timeout=3
        )
        self.assertEqual(queue.bind.call_args.kwargs["arguments"], {"x-match": "all"})
        destination = create_autospec(AbstractRobustExchange, instance=True)
        await bind_exchange(destination, "source", routing_key="orders.#", robust=False)
        self.assertEqual(destination.bind.call_args.args, ("source",))


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_properties_overrides(self) -> None:
        exchange = create_autospec(AbstractExchange, instance=True)
        publisher = RabbitMQPublisher(exchange, timeout=10)
        event = Order(order_id=123)
        await publisher.publish(
            event,
            routing_key="created",
            mandatory=False,
            timeout=None,
            delivery_mode=DeliveryMode.NOT_PERSISTENT,
            headers={"trace": "abc"},
            priority=5,
            correlation_id="request-1",
            reply_to="reply",
            expiration=2.5,
            message_id="custom",
            timestamp=None,
            message_type="order.v2",
            user_id="app",
            app_id="orders",
        )
        message = exchange.publish.call_args.args[0]
        self.assertEqual(Order.model_validate_json(message.body), event)
        self.assertEqual(message.headers, {"trace": "abc"})
        self.assertEqual(message.priority, 5)
        self.assertEqual(message.correlation_id, "request-1")
        self.assertEqual(message.reply_to, "reply")
        self.assertEqual(message.properties.expiration, "2500")
        self.assertEqual(message.message_id, "custom")
        self.assertEqual(message.timestamp, event.occurred_at)
        self.assertEqual(message.type, "order.v2")
        self.assertEqual(message.user_id, "app")
        self.assertEqual(message.app_id, "orders")
        self.assertEqual(message.delivery_mode, DeliveryMode.NOT_PERSISTENT)
        self.assertEqual(exchange.publish.call_args.kwargs["timeout"], 10)
        self.assertFalse(exchange.publish.call_args.kwargs["mandatory"])

    async def test_inherited_defaults_and_raw_message(self) -> None:
        exchange = create_autospec(AbstractExchange, instance=True)
        publisher = RabbitMQPublisher(exchange, timeout=10)
        event = Order(order_id=1)
        await publisher.publish(event, routing_key="created")
        message = exchange.publish.call_args.args[0]
        self.assertEqual(message.message_id, str(event.event_id))
        self.assertEqual(message.timestamp, event.occurred_at)
        self.assertEqual(message.type, "Order")
        self.assertEqual(exchange.publish.call_args.kwargs["timeout"], 10)
        raw = Message(b"not-json", content_type="application/octet-stream")
        await publisher.publish_message(raw, routing_key="raw", timeout=None)
        self.assertIs(exchange.publish.call_args.args[0], raw)
        self.assertEqual(exchange.publish.call_args.kwargs["timeout"], 10)

    async def test_timeout_override_and_disabled_default(self) -> None:
        exchange = create_autospec(AbstractExchange, instance=True)
        publisher = RabbitMQPublisher(exchange, timeout=None)
        event = Order(order_id=1)
        await publisher.publish(event, routing_key="created")
        self.assertIsNone(exchange.publish.call_args.kwargs["timeout"])
        await publisher.publish(event, routing_key="created", timeout=2)
        self.assertEqual(exchange.publish.call_args.kwargs["timeout"], 2)


class ConsumerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.channel = create_autospec(AbstractRobustChannel, instance=True)
        self.queue = create_autospec(AbstractRobustQueue, instance=True)
        self.queue.consume.return_value = "tag"

    async def test_success_and_subscription_options(self) -> None:
        handler = AsyncMock()
        consumer = RabbitMQConsumer(self.channel, self.queue, Order, handler)
        await consumer.start(
            prefetch_count=0,
            prefetch_size=1024,
            global_=True,
            qos_timeout=2,
            exclusive=True,
            priority=3,
            consumer_tag="custom",
            timeout=4,
            robust=False,
        )
        self.channel.set_qos.assert_awaited_once_with(
            prefetch_count=0, prefetch_size=1024, global_=True, timeout=2
        )
        options = self.queue.consume.call_args.kwargs
        self.assertEqual(options["arguments"], {"x-priority": 3})
        self.assertFalse(options["robust"])
        self.assertTrue(options["exclusive"])
        message = incoming()
        await consumer._on_message(message)
        handler.assert_awaited_once()
        message.ack.assert_awaited_once()
        message.reject.assert_not_awaited()
        await consumer.stop(cancel_timeout=2, nowait=True)
        self.queue.cancel.assert_awaited_once_with("tag", timeout=2, nowait=True)

    async def test_no_ack_never_settles_even_on_failure(self) -> None:
        for body, failing in [(None, False), (b"invalid", False), (None, True)]:
            with self.subTest(body=body, failing=failing):
                handler = AsyncMock(
                    side_effect=RuntimeError("failure") if failing else None
                )
                consumer = RabbitMQConsumer(self.channel, self.queue, Order, handler)
                await consumer.start(no_ack=True)
                message = incoming(body)
                if body is not None or failing:
                    with self.assertLogs("rabbitmq.consumer", level="ERROR"):
                        await consumer._on_message(message)
                else:
                    await consumer._on_message(message)
                message.ack.assert_not_awaited()
                message.reject.assert_not_awaited()
                await consumer.stop()

    async def test_error_policies(self) -> None:
        for invalid in [False, True]:
            for requeue in [False, True]:
                with self.subTest(invalid=invalid, requeue=requeue):
                    handler = AsyncMock(side_effect=RuntimeError("failure"))
                    consumer = RabbitMQConsumer(
                        self.channel,
                        self.queue,
                        Order,
                        handler,
                        requeue_on_error=requeue,
                        requeue_on_invalid=requeue,
                    )
                    message = incoming(b"invalid" if invalid else None)
                    with self.assertLogs("rabbitmq.consumer", level="ERROR"):
                        await consumer._on_message(message)
                    message.reject.assert_awaited_once_with(requeue=requeue)
                    message.ack.assert_not_awaited()
                    if invalid:
                        handler.assert_not_awaited()

    async def test_manual_ack_is_not_duplicated(self) -> None:
        async def handle(event, message):
            await message.ack()
            message.processed = True

        consumer = RabbitMQConsumer(
            self.channel, self.queue, Order, message_handler=handle, auto_ack=False
        )
        message = incoming()
        await consumer._on_message(message)
        message.ack.assert_awaited_once()
        message.reject.assert_not_awaited()
        with self.assertRaisesRegex(ValueError, "no_ack"):
            await consumer.start(no_ack=True)

    async def test_already_settled_message_is_not_rejected_on_later_error(self) -> None:
        async def handle(event, message):
            await message.ack()
            message.processed = True
            raise RuntimeError("after ack")

        consumer = RabbitMQConsumer(
            self.channel, self.queue, Order, message_handler=handle
        )
        message = incoming()
        with self.assertLogs("rabbitmq.consumer", level="ERROR"):
            await consumer._on_message(message)
        message.ack.assert_awaited_once()
        message.reject.assert_not_awaited()

    async def test_concurrent_start_creates_one_subscription(self) -> None:
        consumer = RabbitMQConsumer(self.channel, self.queue, Order, AsyncMock())
        results = await asyncio.gather(
            consumer.start(), consumer.start(), return_exceptions=True
        )
        self.assertEqual(sum(isinstance(result, RuntimeError) for result in results), 1)
        self.queue.consume.assert_awaited_once()
        await consumer.stop()

    async def test_drain_timeout_preserves_task_and_can_be_retried(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        async def handler(event):
            entered.set()
            await release.wait()

        consumer = RabbitMQConsumer(self.channel, self.queue, Order, handler)
        await consumer.start()
        message = incoming()
        task = asyncio.create_task(consumer._on_message(message))
        await asyncio.wait_for(entered.wait(), 1)
        try:
            with self.assertRaises(TimeoutError):
                await consumer.stop(timeout=0)
            self.assertFalse(task.done())
            with self.assertRaises(RuntimeError):
                await consumer.start()
            release.set()
            await consumer.stop(timeout=1)
            await task
            message.ack.assert_awaited_once()
            self.queue.cancel.assert_awaited_once()
        finally:
            release.set()
            await task

    async def test_stop_inside_handler_fails_without_deadlock(self) -> None:
        async def handler(event):
            with self.assertRaisesRegex(RuntimeError, "вне обработчика"):
                await consumer.stop()

        consumer = RabbitMQConsumer(self.channel, self.queue, Order, handler)
        await asyncio.wait_for(consumer._on_message(incoming()), 1)

    def test_invalid_handler_configuration(self) -> None:
        with self.assertRaises(ValueError):
            RabbitMQConsumer(self.channel, self.queue, Order)
        with self.assertRaises(ValueError):
            RabbitMQConsumer(
                self.channel, self.queue, Order, AsyncMock(), auto_ack=False
            )
        with self.assertRaises(ValueError):
            RabbitMQConsumer(
                self.channel,
                self.queue,
                Order,
                AsyncMock(),
                message_handler=AsyncMock(),
            )


if __name__ == "__main__":
    unittest.main()
