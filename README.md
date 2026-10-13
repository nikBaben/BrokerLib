# BrokerLib

Асинхронная библиотека RabbitMQ на `aio-pika` и Pydantic. Требуются Python 3.14, Poetry и Docker Compose.

## Установка

Из корня репозитория:

```sh
cd RabbitMQ
poetry install
# Только при первом запуске, если .env ещё нет:
cp -n .env.example .env
docker compose up -d --wait
```

Все следующие команды запускаются из каталога `RabbitMQ`. Файл `.env` читается из текущего рабочего каталога. Локальные логин и пароль в `.env.example` соответствуют `compose.yaml`.

## Запуск примеров

Сначала запустите обработчик в первом терминале:

```sh
poetry run python -m examples.worker
```

Затем во втором терминале, также из `RabbitMQ`, отправьте событие:

```sh
poetry run python -m examples.producer
```

Producer напечатает `Событие отправлено`, worker — `Обработка заказа: 123` и `Заказ обработан`. Остановить worker можно через Ctrl+C.

Worker создаёт очередь и привязку. Его нужно запустить до первой публикации: publisher использует `mandatory=True`, поэтому публикация без подходящей очереди завершится ошибкой маршрутизации.

Панель RabbitMQ: http://localhost:15672 (локальные учётные данные: `app` / `app_password`).

## Импорты и VS Code

Библиотека устанавливается Poetry как пакет `rabbitmq`:

```python
from rabbitmq.schemas import Event
from rabbitmq.contracts import EventPublisher
from rabbitmq.config import RabbitMQSettings
from rabbitmq.connection import RabbitMQConnection
from rabbitmq.consumer import RabbitMQConsumer
from rabbitmq.publisher import RabbitMQPublisher
```

Откройте папку `BrokerLib` в VS Code. В локальной конфигурации задан интерпретатор `RabbitMQ/.venv/bin/python`; в Run and Debug доступны `RabbitMQ: worker` и `RabbitMQ: producer` с правильным рабочим каталогом. Если VS Code ранее запомнил другой интерпретатор, выполните **Python: Select Interpreter** и выберите `RabbitMQ/.venv/bin/python`.

Для установки библиотеки в другое Python-окружение: `python -m pip install /путь/к/BrokerLib/RabbitMQ`. При использовании вне примеров задайте `RABBITMQ_URL` через окружение, `.env` в рабочем каталоге или `RabbitMQSettings(url=...)`.

## Использование в других проектах

Установите библиотеку **в окружение того проекта**, где она нужна:

```sh
python -m pip install /Users/baben/Desktop/RabbitMQLib/BrokerLib/RabbitMQ
# Для разработки библиотеки с немедленным применением правок:
python -m pip install -e /Users/baben/Desktop/RabbitMQLib/BrokerLib/RabbitMQ
# Или в проекте с Poetry:
poetry add /Users/baben/Desktop/RabbitMQLib/BrokerLib/RabbitMQ
```

Либо выполните `poetry build` из `RabbitMQ` и установите полученный `.whl` из `RabbitMQ/dist` в другом проекте. Устанавливается только пакет `rabbitmq`: примеры не занимают общее имя `examples` в стороннем окружении. Внутри библиотеки используются абсолютные импорты `from rabbitmq.… import …`.

### Как читать исходный код

Основной путь работы: `connection.py` открывает соединение и канал, `topology.py` объявляет очереди и привязки, `publisher.py` отправляет сообщение, `consumer.py` получает и обрабатывает его.

- `config.py` — настройки из аргументов, окружения и `.env`, включая проверку значений.
- `schemas.py` — базовая модель события с идентификатором и временем.
- `contracts.py` — интерфейс отправителя для проверки типов.
В отправителе значения по умолчанию выбираются обычными `if` до создания сообщения. В обработчике `_on_message` учитывает активные задачи для корректной остановки, а `_process_message` выполняет три шага: проверку формата, вызов вашего обработчика и подтверждение. Комментарии и описания методов написаны на русском; имена Python-методов и параметров оставлены английскими.

### Явные параметры API

В публичных методах библиотеки нет `**kwargs`. Параметры перечислены в сигнатурах и доступны через автодополнение.

| Область | Доступные настройки |
| --- | --- |
| Подключение | URL или host/port/login/password/virtualhost/ssl, heartbeat, connect_timeout, reconnect_interval, fail_fast, name, auth, interleave, happy_eyeballs_delay, SSLContext и файлы сертификатов, client_properties, loop, connection_class |
| Канал | channel_number, publisher_confirms, on_return_raises |
| Exchange | exchange_type, durable, auto_delete, internal, passive, timeout, robust, alternate_exchange, delayed_type, arguments |
| Очередь | durable, exclusive, passive, auto_delete, timeout, robust, queue_type, message_ttl, expires, max_length, max_length_bytes, overflow, dead_letter_exchange, dead_letter_routing_key, max_priority, single_active_consumer, delivery_limit, arguments |
| Привязка | routing_key, arguments, timeout, robust; доступны bind_queue и bind_exchange |
| Публикация | delivery_mode, mandatory, timeout, headers, content_type, content_encoding, priority, correlation_id, reply_to, expiration, message_id, timestamp, message_type, user_id, app_id |
| Потребление | prefetch_count, prefetch_size, global_, qos_timeout, no_ack, exclusive, priority, consumer_tag, timeout, robust, arguments |
| Обработка и остановка | handler или message_handler, auto_ack, requeue_on_error, requeue_on_invalid; stop(timeout, cancel_timeout, nowait) |

`arguments` — именованный словарь расширений AMQP (`x-arguments`), а не произвольные Python-аргументы. Он оставлен для дополнительных параметров брокера и плагинов. Если один ключ указан и именованным параметром, и в `arguments`, библиотека выдаст `ValueError` до обращения к брокеру. Исходный словарь не изменяется. Разные типы очередей поддерживают разные наборы расширений; библиотека не скрывает ошибки совместимости, возвращённые RabbitMQ.

```python
from rabbitmq.topology import bind_queue, declare_exchange, declare_queue

# Внутри async-функции после создания channel:
exchange = await declare_exchange(
    channel,
    "orders.events",
    exchange_type="topic",
    durable=True,
    timeout=5,
)
queue = await declare_queue(
    channel,
    "billing.orders.v1",
    queue_type="quorum",
    durable=True,
    message_ttl=60_000,
    dead_letter_exchange="orders.dead",
    dead_letter_routing_key="order.failed",
    delivery_limit=5,
    single_active_consumer=True,
    timeout=5,
)
await bind_queue(queue, exchange, routing_key="order.created", timeout=5)
```

Для этой схемы отдельно объявите DLX `orders.dead`, очередь ошибок и её привязку. Повторное объявление существующей очереди с несовместимыми аргументами RabbitMQ отклоняет. Для изменения таких настроек используйте подходящую политику брокера или новую очередь.

### Подключение и приоритет настроек

`RabbitMQSettings` поддерживает `.env` и переменные `RABBITMQ_*`. Явные аргументы `RabbitMQConnection` переопределяют поля настроек; переданный объект настроек не изменяется.

```python
from rabbitmq.connection import RabbitMQConnection

# URL можно передать напрямую:
connection = RabbitMQConnection(
    url="amqp://app:app_password@localhost:5672/",
    heartbeat=30,
    connect_timeout=5,
    reconnect_interval=2,
    fail_fast=True,
    name="billing-worker",
)

# Можно указать адрес раздельно — он заменит URL из настроек:
connection = RabbitMQConnection(
    host="localhost",
    port=5672,
    login="app",
    password="app_password",
    virtualhost="/",
    heartbeat=30,
)
```

Явный аргумент `url` имеет приоритет над раздельными параметрами адреса. Если `url` не передан, но задан хотя бы один из host/port/login/password/virtualhost/ssl, используется раздельный адрес. Если не передано ни то, ни другое, адрес берётся из `RabbitMQSettings`. Значения heartbeat/reconnect_interval/fail_fast из настроек или явных аргументов имеют приоритет над одноимёнными query-параметрами URL. `connect_timeout=None` или пропущенный аргумент сохраняет таймаут из настроек. Чтобы отключить его, передайте `RabbitMQSettings(connect_timeout=None)`. При `fail_fast=False` первоначальное подключение может повторяться до успеха; для общего ограничения времени используйте `asyncio.timeout` вокруг `connect()`.

Для TLS используйте URL `amqps://…` (либо `url=None, ssl=True, port=5671`) и `ssl_context` или явные `cafile`, `capath`, `cadata`, `certfile`, `keyfile`, `no_verify_ssl`. `cadata` ожидает base64-строку в формате aiormq. `auth="external"` требует соответствующей настройки брокера и клиентского сертификата.

### Метаданные и подтверждения публикации

```python
from rabbitmq.publisher import RabbitMQPublisher

publisher = RabbitMQPublisher(exchange, mandatory=True, timeout=5)
await publisher.publish(
    event,
    routing_key="order.created",
    headers={"trace_id": "request-123"},
    correlation_id="request-123",
    reply_to="billing.replies",
    expiration=30,  # секунды; message_ttl очереди задаётся в миллисекундах
    app_id="orders-service",
    timeout=2,  # таймаут этой публикации вместо 5 секунд по умолчанию
)
```

Пропущенный `timeout` и `timeout=None` используют значение, заданное при создании publisher. Чтобы отключить таймаут, создайте `RabbitMQPublisher(exchange, timeout=None)`. `message_id`, `timestamp`, `message_type` при значении `None` берутся из события. Чтобы отправить сообщение без этих метаданных, используйте `publish_message` с явно созданным `aio_pika.Message`. `publish` сериализует событие в UTF-8 JSON; поля content_type/content_encoding меняют метаданные. Для другого тела используйте `publish_message(aio_pika.Message(...), routing_key=...)`.

Методы `publish` и `publish_message` возвращают `None`. Если подтверждения включены на канале, они дожидаются ответа брокера; ошибки публикации передаются как исключения. Для явного отказа при отсутствии маршрута создавайте канал с `publisher_confirms=True, on_return_raises=True` и публикуйте с `mandatory=True`. Подтверждение публикации не означает, что обработчик уже выполнил бизнес-операцию.

### Обработка сообщений

Обычный `handler(event)` автоматически подтверждает успешно обработанное сообщение. По умолчанию ошибка валидации или обработчика приводит к `reject(requeue=False)`; при настроенном DLX сообщение попадёт в очередь ошибок. `requeue_on_error=True` и `requeue_on_invalid=True` включают возврат, но сами по себе не реализуют задержку или ограничение повторов.

Для доступа к заголовкам и ручного подтверждения:

```python
from aio_pika.abc import AbstractIncomingMessage
from rabbitmq.consumer import RabbitMQConsumer

async def handle_message(event: OrderCreated, message: AbstractIncomingMessage) -> None:
    print(message.headers, event.order_id)
    await message.ack()

consumer = RabbitMQConsumer(
    channel,
    queue,
    OrderCreated,
    message_handler=handle_message,
    auto_ack=False,
)
await consumer.start(prefetch_count=20, priority=5, timeout=5)
# При завершении приложения, вне обработчика:
await consumer.stop(timeout=30, cancel_timeout=5)
```

`handler` и `message_handler` взаимоисключающие. При `auto_ack=False` обработчик отвечает за ack/reject валидных сообщений, в том числе при своих ошибках; невалидный JSON по-прежнему обрабатывает библиотека. Можно использовать `message_handler` с `auto_ack=True`, чтобы читать метаданные и сохранять автоматическое подтверждение.

`start(no_ack=True)` включает подтверждение на стороне брокера без ожидания обработки: клиент не делает ack/reject даже при ошибке. Этот режим несовместим с ручным подтверждением. `prefetch_count=0` означает отсутствие ограничения по количеству; `global_=True` зависит от поддержки типа очереди (для quorum не используйте глобальный QoS).

`stop` отменяет подписку и ждёт выполняющиеся обработчики. При истечении `timeout` выбрасывается `TimeoutError`, обработчики не отменяются: можно повторить `stop`, либо закрыть канал/соединение, чтобы вернуть неподтверждённые сообщения. `nowait=True` не ждёт подтверждения отмены от брокера, поэтому для штатного завершения оставляйте `False`.

### Границы библиотеки и проверки

В корне репозитория `pyrightconfig.json` задаёт стандартную проверку типов для библиотеки, примеров и тестов. Он указывает на окружение `RabbitMQ/.venv`. В VS Code также настроен запуск `unittest` из каталога `RabbitMQ`; интеграционные тесты без `BROKERLIB_INTEGRATION=1` пропускаются.


Возвращаемые channel/exchange/queue — объекты aio-pika: доступны исходные методы purge/delete/unbind/get, транзакции и другие операции. Настройки кластера, пользователей, политик, лимитов сервера и установка плагинов выполняются средствами RabbitMQ. Устаревший AMQP-флаг `immediate` не поддерживается RabbitMQ и не добавлен в обёртку.

Документация протокола и клиента: [aio-pika API](https://docs.aio-pika.com/apidoc.html), [параметры подключения](https://docs.aio-pika.com/url-parameters.html), [аргументы очередей](https://www.rabbitmq.com/docs/queues).

Из каталога `RabbitMQ`:

```sh
# Без брокера: интеграционные проверки будут пропущены.
poetry run python -m unittest discover -s tests -v

# Полная проверка с локальным RabbitMQ из .env:
BROKERLIB_INTEGRATION=1 poetry run python -m unittest discover -s tests -v
```

Интеграционные тесты создают временные очереди и уникальные exchanges, проверяют метаданные и ручной ack, отправку невалидного JSON в DLX и отказ при отсутствии маршрута. Они не читают прикладные очереди.
