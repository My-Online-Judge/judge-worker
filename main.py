import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from config import Config
from worker import run

logging.basicConfig(level=logging.INFO)


def build_consumer(config):
    return KafkaConsumer(
        config.REQUESTED_TOPIC,
        bootstrap_servers=config.KAFKA_BROKERS.split(","),
        group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def build_producer(config):
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKERS.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


if __name__ == "__main__":
    cfg = Config
    logging.info("judge-worker starting, brokers=%s", cfg.KAFKA_BROKERS)
    run(cfg, build_consumer(cfg), build_producer(cfg))
