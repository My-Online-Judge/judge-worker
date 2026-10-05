import json
import logging

from kafka import KafkaConsumer, KafkaProducer

import sandbox_status
import tracing
from config import Config
from log_context import SubmissionIdFilter
from worker import run, safe_json_deserialize

_handler = logging.StreamHandler()
_handler.addFilter(SubmissionIdFilter())
_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s [%(submission_id)s] %(name)s: %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_handler])


def build_consumer(config):
    return KafkaConsumer(
        config.REQUESTED_TOPIC,
        bootstrap_servers=config.KAFKA_BROKERS.split(","),
        group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=safe_json_deserialize,
        # T2-5: tune for slow, one-at-a-time judging so a busy worker is not mistaken for a dead
        # one and evicted (which causes rebalance churn and duplicate verdicts).
        max_poll_records=config.MAX_POLL_RECORDS,
        max_poll_interval_ms=config.MAX_POLL_INTERVAL_MS,
        session_timeout_ms=config.SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=config.HEARTBEAT_INTERVAL_MS,
    )


def build_producer(config):
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKERS.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


if __name__ == "__main__":
    cfg = Config
    logging.info("judge-worker starting, brokers=%s, tracing=%s",
                 cfg.KAFKA_BROKERS, tracing.init_tracing())
    sandbox_status.start(cfg)
    run(cfg, build_consumer(cfg), build_producer(cfg))
