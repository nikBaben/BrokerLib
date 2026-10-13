# messaging/rabbitmq/connection.py

import asyncio
from typing import Any, Self

import aio_pika

from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
)

from rabbitmq.config import RabbitMQSettings


class RabbitMQConnection:
    def __init__(
        self,
        settings: RabbitMQSettings,
        **connection_options: Any,
    ) -> None:
        self._settings = settings
        self._connection_options = connection_options

        self._connection: AbstractRobustConnection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._lock:
            if (
                self._connection is not None
                and not self._connection.is_closed
            ):
                return

            self._connection = await aio_pika.connect_robust(
                self._settings.url,
                heartbeat=self._settings.heartbeat,
                reconnect_interval=self._settings.reconnect_interval,
                timeout=self._settings.connect_timeout,
                **self._connection_options,
            )

    async def create_channel(
        self,
        *,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
        channel_number: int | None = None,
    ) -> AbstractRobustChannel:
        async with self._lock:
            connection = self._connection

            if connection is None or connection.is_closed:
                raise RuntimeError(
                    "RabbitMQ connection is not established"
                )

            channel = await connection.channel(
                channel_number=channel_number,
                publisher_confirms=publisher_confirms,
                on_return_raises=on_return_raises,
            )

            await channel.ready()
            return channel

    async def close(self) -> None:
        async with self._lock:
            if self._connection is None:
                return

            try:
                await self._connection.close()
            finally:
                self._connection = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        await self.close()