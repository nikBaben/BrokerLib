"""Пример события создания заказа."""

from rabbitmq.schemas import Event


class OrderCreated(Event):
    order_id: int
    user_id: int
