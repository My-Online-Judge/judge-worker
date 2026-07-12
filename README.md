# judge-worker

Kafka consumer bọc quanh qduoj judge-server. Consume `submission.requested`,
gọi judge-server `/judge`, resolve verdict, publish `submission.judged`.

## Run tests
    python -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python -m pytest -v

## Env
- KAFKA_BROKERS (default localhost:9092)
- JUDGE_SERVER_URL (default http://localhost:8080)
- JUDGE_SERVER_TOKEN (default default_token)
- REQUESTED_TOPIC / JUDGED_TOPIC / CONSUMER_GROUP / JUDGE_TIMEOUT_SECONDS
