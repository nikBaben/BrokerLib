from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitMQSettings(BaseSettings):
    """Настройки из аргументов, переменных RABBITMQ_* и файла .env."""

    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_",
        env_file=".env",
        extra="ignore",
        validate_assignment=True,  # Проверяем значения и при изменении полей.
    )

    # Если URL задан, адрес и учётные данные берутся из него.
    # Для раздельных host/port/login/password укажите url=None.
    url: str | None = "amqp://guest:guest@localhost/"
    host: str = "localhost"
    port: int = Field(default=5672, ge=1, le=65535)
    login: str = "guest"
    password: str = Field(default="guest", repr=False)
    virtualhost: str = "/"
    ssl: bool = False

    # Интервалы и таймаут подключения задаются в секундах.
    heartbeat: int = Field(default=60, ge=0)
    reconnect_interval: float = Field(default=5, gt=0)
    connect_timeout: float | None = Field(default=10, gt=0)
    fail_fast: bool = True
    # Максимум неподтверждённых сообщений у обработчика; 0 снимает ограничение.
    prefetch_count: int = Field(default=10, ge=0)
