# judge-worker

Kafka consumer bọc quanh `judge-server` (sandbox viết bằng Go) qua gRPC. Consume `submission.requested`,
gọi `JudgeServer.JudgeStream` của judge-server (mỗi case chấm xong là một event, cuối cùng là verdict), resolve
verdict, publish `submission.judged`; từng case chấm xong được báo lên `submission.progress` để portal tô dần bảng test.
Sandbox chưa có `JudgeStream` (UNIMPLEMENTED) thì worker gọi lại `Judge` như cũ, không báo tiến độ.

## Run tests
    python -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python -m pytest -v

## Env
- KAFKA_BROKERS (default localhost:9092)
- OJ_TLS_DIR (vd /etc/oj/tls) — thư mục chứa `ca.crt`, `tls.crt`, `tls.key`: Kafka qua TLS, trình chứng chỉ client,
  kiểm tra hostname của broker; heartbeat tới submission-service, và các lệnh gọi gRPC `Ping`, `Judge` tới sandbox
  cũng trình chứng chỉ đó; /metrics chạy HTTPS và chỉ trả lời client có chứng chỉ do CA ký (Prometheus). Thiếu file thì worker
  dừng. Không đặt thì plaintext
- JUDGE_SERVER_URL (default http://localhost:8080; compose: https://judge-server:8080,https://judge-server-2:8080) — địa chỉ
  gRPC của sandbox, nhiều sandbox cách nhau bằng dấu phẩy. Mỗi giá trị là `host:port` (vd `judge-server:8080`) hoặc
  `https://host:port`: tiền tố `http://` / `https://` bị bỏ, TLS do OJ_TLS_DIR quyết định chứ không do tiền tố. Không được có
  path, host rỗng hay port sai: worker dừng ngay khi khởi động với `invalid JUDGE_SERVER_URL entry: …` thay vì lỗi ở từng bài nộp.
  Worker round-robin giữa các sandbox và chuyển sang sandbox kế khi `Judge` trả mã gRPC UNAVAILABLE, INTERNAL hoặc
  RESOURCE_EXHAUSTED; mã khác (INVALID_ARGUMENT, DEADLINE_EXCEEDED, …) được trả lên ngay, không failover. Sandbox đã đầy trả
  RESOURCE_EXHAUSTED ngay (deployment đặt `MAX_QUEUE=0`, không xếp hàng chờ), còn DEADLINE_EXCEEDED thì không retry
- JUDGE_SERVER_TOKEN (bắt buộc, không có default: thiếu thì worker dừng ngay khi khởi động)
- REQUESTED_TOPIC / JUDGED_TOPIC / CONSUMER_GROUP / JUDGE_TIMEOUT_SECONDS (default 30) — phần nền của deadline mỗi lần gọi `Judge`
- JUDGE_TIMEOUT_MAX_SECONDS (default 300) — trần của deadline đó. Deadline = JUDGE_TIMEOUT_SECONDS + số case × 3 × max_cpu_time
  (giây, làm tròn lên; bundle không đọc được số case thì tính 1 case) + SPJ_COMPILE_ALLOWANCE_SECONDS nếu request có checker,
  rồi cắt ở trần. Phải nhỏ hơn MAX_POLL_INTERVAL_MS trừ ngân sách MinIO, và nhỏ hơn `stop_grace_period` của container:
  SIGTERM chờ lần chấm đang chạy xong rồi publish verdict, nên `stop_grace_period` phải lớn hơn JUDGE_TIMEOUT_MAX_SECONDS
  cộng thời gian publish (compose đặt 6m)
- SPJ_COMPILE_ALLOWANCE_SECONDS (default 20) — request có checker được cộng chừng này giây vào deadline, vì
  sandbox biên dịch checker ở lần dùng đầu trên mỗi sandbox (tối đa 20 s)
- MAX_POLL_INTERVAL_MS (default 900000, tức 15 phút) — quá thời gian này không `poll()` thì Kafka chuyển partition cho worker
  khác. `process_event` là một đoạn không `poll()`: đồng bộ MinIO (MINIO_TIMEOUT_SECONDS × (MINIO_RETRIES + 1), mặc định
  3 × 60 = 180 s), rồi khi failover thì mỗi sandbox một lần `Judge`, mỗi lần tới JUDGE_TIMEOUT_MAX_SECONDS: 180 + 2 × 300 =
  780 s với hai sandbox, nên 15 phút còn dư. Mỗi sandbox thêm vào JUDGE_SERVER_URL cộng tối đa 300 s: từ ba sandbox phải
  tăng giá trị này hoặc hạ JUDGE_TIMEOUT_MAX_SECONDS. Các lần chờ Kafka, MinIO hay sandbox mất kết nối đều `poll()` mỗi 1 s
  nên không tính vào đây
- STARTED_TOPIC (default submission.started) — worker báo bắt đầu chấm trước mỗi lần dispatch; WORKER_ID (default hostname)
- PROGRESS_TOPIC (default submission.progress) — mỗi case chấm xong một record (key = submissionId): `testCase`, `result`,
  `error`, `cpuTime`, `realTime`, `memory`, `total` (số case của bundle, null nếu không đọc được), `rejudgeCount` nếu
  request có. Không có output của bài; chỉ báo, không chờ ack, lỗi Kafka chỉ ghi log. Khi failover sang sandbox khác,
  case đã báo không bị rút lại: portal đếm theo tên case, verdict cuối mới là câu trả lời đầy đủ
- JUDGE_INFRA_RETRY_SECONDS (default 300) — MinIO hoặc mọi sandbox mất kết nối thì chấm lại sau 2 s, 4 s … tối đa 60 s,
  quá ngân sách này thì SYSTEM_ERROR `Judge infrastructure unavailable`; đếm ở oj_judge_infra_retries_total{reason}. Với sandbox,
  chỉ mã gRPC UNAVAILABLE, INTERNAL, RESOURCE_EXHAUSTED được chờ, trừ UNAVAILABLE mà chi tiết (`details`) nhắc `ssl` hoặc
  `certificate` (không phân biệt hoa thường): đó là cấu hình TLS sai vĩnh viễn (CA hoặc hostname sai, không tự khỏi), gRPC lại
  báo bằng UNAVAILABLE như sandbox đang tắt, nên không retry: SYSTEM_ERROR ngay thay vì chờ hết ngân sách. UNAVAILABLE khác
  (từ chối kết nối, socket đóng, bị reset, lỗi DNS) và INTERNAL, RESOURCE_EXHAUSTED vẫn được chờ như cũ; `handshake` không phải
  dấu hiệu (sandbox khởi động lại cắt giữa chừng trông giống lỗi bắt tay). Giới hạn: với TLS 1.3, chứng chỉ client bị sandbox từ
  chối chỉ hiện là `Socket closed` sau bắt tay, giống sandbox đang khởi động lại, nên vẫn được chờ. Không retry khi
  DEADLINE_EXCEEDED (sandbox có thể đang chấm) hay mã khác (INVALID_ARGUMENT, …). Với MinIO, lỗi chứng chỉ TLS (urllib3
  SSLError) cũng vĩnh viễn: không retry. Lỗi không retry thành SYSTEM_ERROR với `error_message` một dòng `MÃ_GRPC: chi tiết`
  (tối đa 500 ký tự)
- METRICS_PORT (default 9100) — Prometheus /metrics: oj_sandbox_up, oj_judge_dispatch_total, oj_run_expired_total
- SANDBOX_HEARTBEAT_URL (vd https://submission-service:8000/api/judge_server_heartbeat/) — worker gọi gRPC `Ping` của
  từng sandbox rồi chuyển câu trả lời (hostname, judger_version, cpu_core, cpu, memory, running_jobs, max_jobs) về registry
  thay cho sandbox (sandbox ở mạng internal, không tới được submission-service); không đặt thì tắt (vẫn `Ping` để cập
  nhật oj_sandbox_up). SANDBOX_HEARTBEAT_INTERVAL_SECONDS (default 10)
- OTEL_EXPORTER_OTLP_ENDPOINT (vd http://jaeger:4318) — bật tracing (OTLP/HTTP); không đặt thì tracing tắt hoàn toàn
- OTEL_SERVICE_NAME (default judge-worker) — tên service trong Jaeger; `run-worker` đặt tên riêng của nó

## Làn Run (Run code design §8)
Một record có `runId` + `test_case` (không có `test_case_id`) là một *run*: worker không đồng bộ bundle, không báo
`submission.started` hay `submission.progress` (gọi `Judge` thường, không stream), gửi `test_case` thẳng cho sandbox và
trả verdict lên `JUDGED_TOPIC` dưới `runId` (cùng envelope với
`submission.judged`). Run có `expiresAt` (epoch ms) đã qua thì bị bỏ qua và đếm ở `oj_run_expired_total` — sau một
đợt làn Run gián đoạn, hàng đợi cũ được dọn ngay thay vì chấm cho không ai. Compose chạy service `run-worker` từ chính
image này với `REQUESTED_TOPIC=run.requested JUDGED_TOPIC=run.judged CONSUMER_GROUP=run-workers OTEL_SERVICE_NAME=run-worker`
và `JUDGE_INFRA_RETRY_SECONDS=30`, không MinIO, không heartbeat relay.

## Bộ test trong /test_case
Bài không có checker: thư mục bộ test giữ 0700, chỉ root của sandbox đọc được. Bài có checker (`"spj": true` trong
`info`): thư mục và các `*.in` thuộc group 12003 (group của user `spj` (uid 12003) của sandbox, user chạy checker), 0750 / 0640; `*.out` và `info`
0600. Bài nộp chạy với user riêng của từng slot (uid 12100-12107), checker chạy với user `spj` (uid 12003) và group của slot; bài nộp không đọc được gì trong bộ test. Vì vậy worker phải ở trong group 12003:
compose đặt `group_add: ["12003"]`; chạy ngoài compose thì `docker run --group-add 12003`.
