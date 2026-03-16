from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "digital_hunter"

    # Kafka
    KAFKA_BROKER: str = "localhost:9092"
    CONSUME_TOPIC: str = "DEFAULT_TOPIC"
    PRODUCE_TOPIC: str = "DEFAULT_TOPIC"
    GROUP_ID: str = "default_group"

    # Postgres
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_DATABASE: str = "digital_hunter"

    # Elasticsearch
    ELASTICSEARCH_HOST: str = "localhost"


settings = Settings()
