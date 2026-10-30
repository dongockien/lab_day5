# Lab Ngày 5: Hàng đợi, idempotency và outbox với RabbitMQ

API nhận ảnh, đẩy job vào RabbitMQ, worker giả lập "suy luận" mất 800 ms và ghi kết quả vào Postgres. Lab dùng để đo và kiểm chứng bằng số liệu:

| # | Thí nghiệm | Kiểm chứng |
|---|---|---|
| 1 | 1 worker so với 4 worker | Thông lượng và độ trễ p50/p95/p99 khi scale consumer |
| 2 | Kill worker giữa chừng | Không mất job (at-least-once, ack sau commit) |
| 3 | Gửi trùng một job | Job chỉ xử lý một lần (consumer idempotent) |
| 4 | RabbitMQ chết lúc nhận job | So sánh `direct` (mất job) với `outbox` (không mất job) |

Suy luận chỉ là `sleep`: lab không dùng GPU thật và không có hàng đợi ưu tiên.

## Kiến trúc

```
client ─POST /jobs─► api ─(direct)──────────────► rabbitmq ──► worker × N ─► postgres
                       └─(outbox)─► postgres ─► relay ─┘                    (jobs, results,
                                                                             inference_runs)
```

| Service | Vai trò |
|---|---|
| `api` | FastAPI: nhận ảnh, ghi job, publish trực tiếp hoặc ghi outbox. Có `/stats` cho công cụ đo |
| `rabbitmq` | Queue `jobs` durable, message persistent, publisher confirm, ack thủ công, prefetch 1 |
| `worker` | Claim job (có lease), suy luận 800 ms, ghi kết quả, ack sau commit |
| `relay` | Chế độ outbox: đọc bảng `outbox` rồi phát sang RabbitMQ |
| `postgres` | `jobs`, `results` (khóa chính `job_id`), `inference_runs`, `outbox` |

Cấu hình qua biến môi trường trong `docker-compose.yml`:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `INFER_MS` | 800 | Thời gian suy luận giả lập |
| `LEASE_SECONDS` | 5 | Hạn lease của job đang xử lý; phải lớn hơn thời gian xử lý tối đa |
| `PUBLISH_MODE` | `direct` | `direct` hoặc `outbox` |

Tài khoản (`postgres/secret`, `lab/lab`) chỉ dùng cho lab. Các cổng chỉ mở trên `127.0.0.1`.

## Cấu trúc thư mục

```
lab_day5/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── app/
│   ├── schema.sql      # bảng jobs, results, inference_runs, outbox
│   ├── common.py       # cấu hình, backoff + jitter, Publisher (confirm)
│   ├── api.py          # POST /jobs, GET /jobs/{id}, /stats, /admin/reset
│   ├── worker.py       # consumer idempotent (claim + lease, ack sau commit)
│   ├── relay.py        # outbox relay (FOR UPDATE SKIP LOCKED)
│   ├── client.py       # hàm gọi API cho công cụ đo
│   ├── bench.py        # thí nghiệm 1
│   └── check.py        # thí nghiệm 2, 3, 4
└── scripts/
    ├── lib.sh
    ├── run_bench.sh    # thí nghiệm 1  → results/bench.txt
    ├── run_kill.sh     # thí nghiệm 2  → results/kill.txt
    ├── run_dup.sh      # thí nghiệm 3  → results/dup.txt
    └── run_outbox.sh   # thí nghiệm 4  → results/outbox.txt
```

## Yêu cầu

- Docker và Docker Compose v2 (lệnh `docker compose`, hỗ trợ `up --wait`)
- Bash (Git Bash trên Windows, hoặc terminal Linux/macOS)
- Các cổng 5432, 5672, 15672, 8000 trên máy host chưa bị chiếm

Không cần cài Python trên máy host: mọi thứ chạy trong container.

## Lấy mã nguồn

```bash
git clone https://github.com/dongockien/lab_day5.git
cd lab_day5
```

## Chạy

Chạy từng thí nghiệm, mỗi lệnh tự dựng hệ thống, đo và lưu kết quả vào `results/`:

```bash
bash scripts/run_bench.sh      # ~2 phút (build image lần đầu lâu hơn)
bash scripts/run_kill.sh
bash scripts/run_dup.sh
bash scripts/run_outbox.sh
```

Giao diện quản trị RabbitMQ: http://localhost:15672 (`lab` / `lab`), xem queue `jobs`.

### Thí nghiệm 1: scale worker

Gửi 40 job cùng lúc, đo với 1 rồi 4 worker. Kết quả mẫu cần đọc:

```
total_time_s, throughput_jobs_s (kèm trần lý thuyết), latency_avg_s, p50, p95, p99, max
```

Kỳ vọng: 4 worker nhanh gần 4 lần; độ trễ trung bình khác rõ với p95/p99 ở cấu hình 1 worker (job cuối hàng đợi chờ rất lâu trong khi trung bình trông ổn).

### Thí nghiệm 2: kill worker

Script gửi 40 job với 4 worker, sau 2 giây chạy `docker kill` (SIGKILL) một worker. Đạt khi in `RESULT PASS (không mất job)`:

| Chỉ số | Kỳ vọng |
|---|---|
| `lost_jobs` | 0 |
| `redelivered_jobs` | ≥ 1 (job đang chạy trên worker bị kill) |
| `max_attempts` | 2 |
| `inference_runs` | `submitted` + số job bị làm lại |

Job bị giao lại sẽ chờ tới khi lease (5 s) của worker đã chết hết hạn mới được xử lý lại, nên `latency_max` tăng thêm cỡ lease.

### Thí nghiệm 3: gửi trùng

Hai request đồng thời cùng `Idempotency-Key`, rồi một request thứ ba sau khi job đã xong. API cố ý không loại trùng để kiểm tra phía consumer. Đạt khi `result_rows = 1` và `inference_runs = 1` sau cả ba request.

### Thí nghiệm 4: RabbitMQ chết

Mỗi chế độ: dừng RabbitMQ, gửi 5 job, bật lại RabbitMQ, chờ xử lý.

| Chế độ | HTTP | Kết quả sau khi RabbitMQ chạy lại |
|---|---|---|
| `direct` | 503 × 5 | `lost_jobs = 5`, job kẹt ở `queued` (đã ghi DB nhưng không bao giờ vào queue) |
| `outbox` | 202 × 5 | `lost_jobs = 0`, relay phát lại các sự kiện còn trong outbox |

## Kết quả đã đo

Môi trường: Windows, Docker Desktop, Git Bash; RabbitMQ 3.13, PostgreSQL 16, Python 3.12; mỗi kịch bản chạy một lần. Chạy cục bộ nên chỉ dùng để so sánh tương đối.

**Thí nghiệm 1: 40 job gửi cùng lúc**

| Chỉ số | 1 worker | 4 worker |
|---|---|---|
| Tổng thời gian | 32,7 s | 8,4 s |
| Thông lượng | 1,22 job/s (tối đa 1,25) | 4,77 job/s (tối đa 5,0) |
| Độ trễ trung bình | 16,38 s | 4,38 s |
| p50 / p95 / p99 | 16,40 / 30,31 / 31,55 s | 4,39 / 7,94 / 7,94 s |

**Thí nghiệm 2: kill worker** (4 worker, 40 job): `lost_jobs = 0`, `redelivered_jobs = 1`, `max_attempts = 2`, `inference_runs = 41`, `latency_max = 11,60 s`.

**Thí nghiệm 3: gửi trùng**: sau cả ba request `result_rows = 1`, `inference_runs = 1`, `attempts = 1`.

**Thí nghiệm 4: RabbitMQ chết lúc nhận 5 job**

| Chế độ | HTTP | `results_rows` | `jobs_stuck_queued` | `lost_jobs` |
|---|---|---|---|---|
| `direct` | 503 × 5 | 0 | 5 | 5 |
| `outbox` | 202 × 5 | 5 | 0 | 0 |

Kết quả thô nằm trong thư mục `results/`.

## Thiết kế chống mất và chống trùng

Worker xử lý mỗi message theo thứ tự:

1. **Claim:** cập nhật có điều kiện `jobs` sang `processing` kèm `lease_until`. Chỉ thành công khi job đang `queued` hoặc lease của người giữ đã hết hạn.
2. Không claim được vì job đã `done`: ack và bỏ qua. Vì job đang được worker khác giữ: nghỉ 0,5 s rồi trả message về queue (không ack).
3. Chạy suy luận, ghi dòng vào `inference_runs`.
4. Trong một transaction: ghi `results` (`ON CONFLICT DO NOTHING`) và đặt `jobs.status = 'done'`.
5. **Ack sau khi commit.**

Hệ quả: worker chết ở bất kỳ bước nào cũng không làm mất job; suy luận có thể chạy lại (đếm ở `inference_runs`) nhưng kết quả chỉ có một dòng. Lease phải lớn hơn thời gian xử lý tối đa, nếu không job đang chạy có thể bị worker khác nhận lại.

## Xử lý lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `$'\r': command not found` khi chạy script | File `.sh` bị đổi sang CRLF khi clone trên Windows | Giữ `.gitattributes` (`*.sh text eol=lf`) hoặc chạy `dos2unix scripts/*.sh` |
| `chưa đủ N worker kết nối RabbitMQ` | Worker chưa kịp kết nối | Chạy lại; kiểm tra `docker compose logs worker` |
| `up --wait` báo lỗi | Docker Compose cũ | Cập nhật Docker Desktop/Compose v2 |
| Cổng bị chiếm | Dịch vụ khác dùng 5432/5672/15672/8000 | Dừng dịch vụ đó hoặc đổi cổng trong `docker-compose.yml` |
| Không thấy job nào được xử lý | RabbitMQ chưa healthy | `docker compose ps`, đợi `rabbitmq` ở trạng thái healthy |

Xem log: `docker compose logs -f worker api relay`. Số liệu tức thời: `curl localhost:8000/stats`.

## Dọn dẹp

```bash
docker compose down        # giữ dữ liệu
docker compose down -v     # xóa cả volume để chạy lại từ đầu
```
