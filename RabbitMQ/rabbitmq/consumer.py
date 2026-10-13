# messaging/rabbitmq/consumer.py

import asyncio
import logging

from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustChannel,
    AbstractRobustQueue,
)
from pydantic import ValidationError

from ..schemas import Event


logger = logging.getLogger(__name__)

EventT = TypeVar("EventT", bound=Event)

EventHandler = Callable[[EventT], Awaitable[None]]


class RabbitMQConsumer(Generic[EventT]):
    def __init__(
        self,
        channel: AbstractRobustChannel,
        queue: AbstractRobustQueue,
        event_type: type[EventT],
        handler: EventHandler[EventT],
    ) -> None:
        self._channel = channel
        self._queue = queue
        self._event_type = event_type
        self._handler = handler

        self._consumer_tag: str | None = None
        self._active_tasks: set[asyncio.Task[None]] = set()

    async def start(
        self,
        *,
        prefetch_count: int = 10,
        consumer_tag: str | None = None,
    ) -> None:
        if self._consumer_tag is not None:
            raise RuntimeError("Consumer already started")

        await self._channel.set_qos(
            prefetch_count=prefetch_count,
        )

        self._consumer_tag = await self._queue.consume(
            self._on_message,
            no_ack=False,
            consumer_tag=consumer_tag,
        )

    async def _on_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        task = asyncio.current_task()

        if task is not None:
            self._active_tasks.add(task)

        try:
            try:
                event = self._event_type.model_validate_json(
                    message.body,
                )
            except (ValidationError, ValueError):
                logger.exception(
                    "Invalid event: %s",
                    self._event_type.__name__,
                )

                await message.reject(requeue=False)
                return

            try:
                await self._handler(event)
            except Exception:
                logger.exception(
                    "Handler failed: event_id=%s",
                    event.event_id,
                )

                await message.reject(requeue=False)
                return

            await message.ack()

        finally:
            if task is not None:
                self._active_tasks.discard(task)

    async def stop(
        self,
        *,
        timeout: float = 30.0,
    ) -> None:
        if self._consumer_tag is None:
            return

        await self._queue.cancel(self._consumer_tag)
        self._consumer_tag = None

        if self._active_tasks:
            await asyncio.wait_for(
                asyncio.gather(*tuple(self._active_tasks)),
                timeout=timeout,
            )