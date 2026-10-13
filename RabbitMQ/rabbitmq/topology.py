from typing import Literal

from aio_pika import ExchangeType
from aio_pika.abc import (
    AbstractExchange,
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
    HeadersType,
)


async def declare_exchange(
    channel: AbstractRobustChannel,
    name: str,
    *,
    exchange_type: ExchangeType | str = ExchangeType.TOPIC,
    durable: bool = True,
    auto_delete: bool = False,
    internal: bool = False,
    passive: bool = False,
    arguments: HeadersType | None = None,
    timeout: float | None = None,
    robust: bool = True,
    alternate_exchange: str | None = None,
    delayed_type: str | None = None,
) -> AbstractRobustExchange:
    """Объявить exchange с указанными свойствами.

    passive=True проверяет существующий exchange, robust=True восстанавливает
    его при переподключении. delayed_type требует плагин delayed-message.
    """
    # Копия защищает словарь вызывающего кода от изменений.
    exchange_arguments = dict(arguments or {})
    named_arguments: HeadersType = {
        "alternate-exchange": alternate_exchange,
        "x-delayed-type": delayed_type,
    }
    for key, value in named_arguments.items():
        if value is None:
            continue
        if key in exchange_arguments:
            raise ValueError(
                f"Настройка {key} задана и отдельным параметром, и в arguments"
            )
        exchange_arguments[key] = value

    return await channel.declare_exchange(
        name=name,
        type=exchange_type,
        durable=durable,
        auto_delete=auto_delete,
        internal=internal,
        passive=passive,
        arguments=exchange_arguments or None,
        timeout=timeout,
        robust=robust,
    )


async def declare_queue(
    channel: AbstractRobustChannel,
    name: str | None = None,
    *,
    durable: bool = True,
    exclusive: bool = False,
    passive: bool = False,
    auto_delete: bool = False,
    arguments: HeadersType | None = None,
    timeout: float | None = None,
    robust: bool = True,
    queue_type: Literal["classic", "quorum", "stream"] | None = None,
    message_ttl: int | None = None,
    expires: int | None = None,
    max_length: int | None = None,
    max_length_bytes: int | None = None,
    overflow: Literal["drop-head", "reject-publish", "reject-publish-dlx"]
    | None = None,
    dead_letter_exchange: str | None = None,
    dead_letter_routing_key: str | None = None,
    max_priority: int | None = None,
    single_active_consumer: bool | None = None,
    delivery_limit: int | None = None,
) -> AbstractRobustQueue:
    """Объявить очередь с указанными свойствами и расширениями.

    message_ttl и expires задаются в миллисекундах. Поддержка расширений зависит
    от типа очереди. Дополнительные расширения передаются через arguments.
    Повтор одного ключа в arguments и именованных параметрах вызывает ValueError.
    """
    # Копия защищает словарь вызывающего кода от изменений.
    queue_arguments = dict(arguments or {})
    named_arguments: HeadersType = {
        "x-queue-type": queue_type,
        "x-message-ttl": message_ttl,
        "x-expires": expires,
        "x-max-length": max_length,
        "x-max-length-bytes": max_length_bytes,
        "x-overflow": overflow,
        "x-dead-letter-exchange": dead_letter_exchange,
        "x-dead-letter-routing-key": dead_letter_routing_key,
        "x-max-priority": max_priority,
        "x-single-active-consumer": single_active_consumer,
        "x-delivery-limit": delivery_limit,
    }
    for key, value in named_arguments.items():
        if value is None:
            continue
        if key in queue_arguments:
            raise ValueError(
                f"Настройка {key} задана и отдельным параметром, и в arguments"
            )
        queue_arguments[key] = value

    return await channel.declare_queue(
        name=name,
        durable=durable,
        exclusive=exclusive,
        passive=passive,
        auto_delete=auto_delete,
        arguments=queue_arguments or None,
        timeout=timeout,
        robust=robust,
    )


async def bind_queue(
    queue: AbstractRobustQueue,
    exchange: AbstractExchange | str,
    *,
    routing_key: str | None = None,
    arguments: HeadersType | None = None,
    timeout: float | None = None,
    robust: bool = True,
) -> None:
    """Привязать очередь к exchange по routing_key и дополнительным условиям."""
    await queue.bind(
        exchange,
        routing_key=routing_key,
        arguments=arguments,
        timeout=timeout,
        robust=robust,
    )


async def bind_exchange(
    destination: AbstractRobustExchange,
    source: AbstractExchange | str,
    *,
    routing_key: str = "",
    arguments: HeadersType | None = None,
    timeout: float | None = None,
    robust: bool = True,
) -> None:
    """Привязать exchange-получатель к exchange-источнику."""
    await destination.bind(
        source,
        routing_key=routing_key,
        arguments=arguments,
        timeout=timeout,
        robust=robust,
    )
