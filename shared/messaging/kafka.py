import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


def get_brokers_from_env() -> str:
    return os.getenv("KAFKA_BROKERS", "localhost:9092")


async def create_producer(brokers: str | None = None, retries: int = 20, backoff_sec: float = 0.5) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=brokers or get_brokers_from_env(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all",
    )
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            await producer.start()
            return producer
        except Exception as e:
            last_err = e
            logging.warning("Kafka producer start failed (attempt %s/%s): %s", attempt + 1, retries, e)
            await asyncio.sleep(backoff_sec * (attempt + 1))
    # final try to raise
    if last_err:
        raise last_err
    return producer


async def create_consumer(topic: str, group_id: str, brokers: str | None = None, retries: int = 20, backoff_sec: float = 0.5) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers or get_brokers_from_env(),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            await consumer.start()
            return consumer
        except Exception as e:
            last_err = e
            logging.warning("Kafka consumer start failed (attempt %s/%s): %s", attempt + 1, retries, e)
            await asyncio.sleep(backoff_sec * (attempt + 1))
    if last_err:
        raise last_err
    return consumer


