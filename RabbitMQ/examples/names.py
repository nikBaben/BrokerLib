"""Имена RabbitMQ, общие для отправителя и обработчика примеров."""

from rabbitmq.names import ExchangeName, QueueName, RoutingKey


class Exchanges(ExchangeName):
    ORDERS = "orders.events"


class Queues(QueueName):
    PAYMENTS_ORDERS = "payments.orders"


class RoutingKeys(RoutingKey):
    ORDER_CREATED = "order.created"
