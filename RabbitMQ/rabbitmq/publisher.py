# messaging/rabbitmq/publisher.py

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractExchange

from ..schemas import Event


class RabbitMQPublisher:
    def __init__(
        self,
        exchange: AbstractExchange,
        *,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        mandatory: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._exchange = exchange
        self._delivery_mode = delivery_mode
        self._mandatory = mandatory
        self._timeout = timeout

    async def publish(
        self,
        event: Event,
        *,
        routing_key: str,
    ) -> None:
        message = Message(
            body=event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            delivery_mode=self._delivery_mode,
            message_id=str(event.event_id),
            timestamp=event.occurred_at,
            type=type(event).__name__,
        )

        await self.publish_message(
            message,
            routing_key=routing_key,
        )

    async def publish_message(
        self,
        message: Message,
        *,
        routing_key: str,
        mandatory: bool | None = None,
        timeout: float | None = None,
    ) -> None:
        await self._exchange.publish(
            message,
            routing_key=routing_key,
            mandatory=(
                self._mandatory
                if mandatory is None
                else mandatory
            ),
            timeout=(
                self._timeout
                if timeout is None
                else timeout
            ),
        )