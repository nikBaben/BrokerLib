# messaging/contracts.py

from typing import Protocol

from schemas import Event


class EventPublisher(Protocol):
    async def publish(
        self,
        event: Event,
        *,
        routing_key: str,
    ) -> None:
        ...