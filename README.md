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
  kiểm tra hostname của broker; heartbeat tới submission-service và các lệnh gọi sandbox (`/judge`, `/ping`) cũng trình
  chứng chỉ đó; /metrics chạy HTTPS và chỉ trả lời client có chứng chỉ do CA ký (Prometheus). Thiếu file thì worker
  dừng. Không đặt thì plaintext
- JUDGE_SERVER_URL (default http://localhost:8080; compose: https://judge-server:8080,https://judge-server-2:8080)
- JUDGE_SERVER_TOKEN (bắt buộc, không có default: thiếu thì worker dừng ngay khi khởi động)
- REQUESTED_TOPIC / JUDGED_TOPIC / CONSUMER_GROUP / JUDGE_TIMEOUT_SECONDS
- SPJ_COMPILE_ALLOWANCE_SECONDS (default 20) — request có checker được chờ JUDGE_TIMEOUT_SECONDS + chừng này giây, vì
  sandbox biên dịch checker ở lần dùng đầu (qduoj: tối đa 20 s); MAX_POLL_INTERVAL_MS mặc định
  (JUDGE_TIMEOUT_SECONDS + SPJ_COMPILE_ALLOWANCE_SECONDS) × 1000 + 30000
- STARTED_TOPIC (default submission.started) — worker báo bắt đầu chấm trước mỗi lần dispatch; WORKER_ID (default hostname)
- JUDGE_INFRA_RETRY_SECONDS (default 300) — MinIO hoặc mọi sandbox mất kết nối thì chấm lại sau 2 s, 4 s … tối đa 60 s,
  quá ngân sách này thì SYSTEM_ERROR `Judge infrastructure unavailable`; đếm ở oj_judge_infra_retries_total{reason}. Không retry
  khi ReadTimeout (sandbox có thể đang chấm) hay lỗi chứng chỉ TLS (cấu hình sai, không tự khỏi)
- METRICS_PORT (default 9100) — Prometheus /metrics: oj_sandbox_up, oj_judge_dispatch_total
- SANDBOX_HEARTBEAT_URL (vd https://submission-service:8000/api/judge_server_heartbeat/) — worker chuyển `/ping` của
  từng sandbox về registry thay cho sandbox (sandbox ở mạng internal, không tới được submission-service);
  không đặt thì tắt. SANDBOX_HEARTBEAT_INTERVAL_SECONDS (default 10)
- OTEL_EXPORTER_OTLP_ENDPOINT (vd http://jaeger:4318) — bật tracing (OTLP/HTTP); không đặt thì tracing tắt hoàn toàn
- OTEL_SERVICE_NAME (default judge-worker)

## Bộ test trong /test_case
Bài không có checker: thư mục bộ test giữ 0700, chỉ root của sandbox đọc được. Bài có checker (`"spj": true` trong
`info`): thư mục và các `*.in` thuộc group 12003 (user `spj` của qduoj chạy checker), 0750 / 0640; `*.out` và `info`
0600. Bài nộp chạy với user `code` (12002), không đọc được gì trong bộ test. Vì vậy worker phải ở trong group 12003:
compose đặt `group_add: ["12003"]`; chạy ngoài compose thì `docker run --group-add 12003`.
