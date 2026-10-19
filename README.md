# judge-worker

Kafka consumer bọc quanh qduoj judge-server. Consume `submission.requested`,
gọi judge-server `/judge`, resolve verdict, publish `submission.judged`.

## Run tests
    python -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python -m pytest -v

## Env
- KAFKA_BROKERS (default localhost:9092)
- OJ_TLS_DIR (vd /etc/oj/tls) — thư mục chứa `ca.crt`, `tls.crt`, `tls.key`: Kafka qua TLS, trình chứng chỉ client,
  kiểm tra hostname của broker; thiếu file thì worker dừng. Không đặt thì plaintext
- JUDGE_SERVER_URL (default http://localhost:8080)
- JUDGE_SERVER_TOKEN (default default_token)
- REQUESTED_TOPIC / JUDGED_TOPIC / CONSUMER_GROUP / JUDGE_TIMEOUT_SECONDS
- METRICS_PORT (default 9100) — Prometheus /metrics: oj_sandbox_up, oj_judge_dispatch_total
- SANDBOX_HEARTBEAT_URL (vd http://submission-service:8000/api/judge_server_heartbeat/) — worker chuyển `/ping` của
  từng sandbox về registry thay cho sandbox (sandbox ở mạng internal, không tới được submission-service);
  không đặt thì tắt. SANDBOX_HEARTBEAT_INTERVAL_SECONDS (default 10)
- OTEL_EXPORTER_OTLP_ENDPOINT (vd http://jaeger:4318) — bật tracing (OTLP/HTTP); không đặt thì tracing tắt hoàn toàn
- OTEL_SERVICE_NAME (default judge-worker)
