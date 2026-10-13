# examples/events.py

from ..schemas import Event


class OrderCreated(Event):
    order_id: int
    user_id: int