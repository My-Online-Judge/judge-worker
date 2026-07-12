import os


class Config:
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    JUDGE_SERVER_URL = os.getenv("JUDGE_SERVER_URL", "http://localhost:8080")
    JUDGE_SERVER_TOKEN = os.getenv("JUDGE_SERVER_TOKEN", "default_token")
    REQUESTED_TOPIC = os.getenv("REQUESTED_TOPIC", "submission.requested")
    JUDGED_TOPIC = os.getenv("JUDGED_TOPIC", "submission.judged")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "judge-workers")
    JUDGE_TIMEOUT_SECONDS = int(os.getenv("JUDGE_TIMEOUT_SECONDS", "30"))
