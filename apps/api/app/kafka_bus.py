import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from app.config import settings

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def start_producer() -> None:
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await _producer.start()
    logger.info("Kafka producer started")


async def stop_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


async def publish(topic: str, event: dict[str, Any], key: str | None = None) -> None:
    if _producer is None:
        raise RuntimeError("Kafka producer not started")
    await _producer.send_and_wait(topic, event, key=key)
