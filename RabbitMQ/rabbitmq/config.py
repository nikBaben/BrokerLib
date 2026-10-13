# messaging/rabbitmq/config.py

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_",
        env_file=".env",
        extra="ignore",
    )

    url: str = "amqp://guest:guest@localhost/"

    heartbeat: int = Field(default=60, ge=0)
    reconnect_interval: float = Field(default=5, gt=0)
    connect_timeout: float = Field(default=10, gt=0)
    prefetch_count: int = Field(default=10, ge=1)