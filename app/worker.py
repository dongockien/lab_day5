"""Worker: nhận job từ RabbitMQ, "suy luận" giả lập INFER_MS, ghi kết quả vào Postgres.

At-least-once: broker có thể giao lại cùng một message, nên worker phải idempotent:
  1. CLAIM: chỉ một worker giành được job (status queued, hoặc processing đã hết lease).
  2. Kết quả và trạng thái 'done' ghi trong 1 transaction; chỉ sau khi COMMIT mới ack.
  3. Job đã 'done' mà nhận lại -> ack và bỏ qua, không chạy lại suy luận.
"""
import hashlib
import json
import os
import signal
import socket
import time

import pika
import psycopg

from .common import (DATABASE_URL, INFER_MS, LEASE_SECONDS, QUEUE, REQUEUE_DELAY, backoff_delay,
                     declare_queue, rabbit_connect, setup_logging)

CLAIM = """
INSERT INTO jobs (job_id, status, worker, lease_until, attempts, started_at)
VALUES (%(id)s, 'processing', %(w)s, now() + make_interval(secs => %(lease)s), 1, now())
ON CONFLICT (job_id) DO UPDATE
   SET status = 'processing', worker = EXCLUDED.worker, lease_until = EXCLUDED.lease_until,
       attempts = jobs.attempts + 1, started_at = COALESCE(jobs.started_at, now())
 WHERE jobs.status = 'queued' OR (jobs.status = 'processing' AND jobs.lease_until < now())
RETURNING attempts
"""

WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
log = setup_logging("worker")
stop = {"flag": False}


def handle(ch, method, body, db):
    try:
        msg = json.loads(body)
        job_id = msg["job_id"]
    except (ValueError, KeyError, TypeError):
        log.warning("bad message, dropped: %r", body[:80])
        ch.basic_nack(method.delivery_tag, requeue=False)
        return

    row = db.execute(CLAIM, {"id": job_id, "w": WORKER_ID, "lease": LEASE_SECONDS}).fetchone()
    if row is None:
        status = db.execute("SELECT status FROM jobs WHERE job_id = %s", (job_id,)).fetchone()[0]
        if status == "done":
            log.info("skip job=%s: đã xử lý xong, bỏ qua bản giao trùng", job_id)
            ch.basic_ack(method.delivery_tag)
        else:
            # Worker khác đang giữ job (còn lease). Không ack để không mất job nếu worker đó chết.
            log.info("job=%s đang được worker khác xử lý, trả về hàng đợi", job_id)
            time.sleep(REQUEUE_DELAY)
            ch.basic_nack(method.delivery_tag, requeue=True)
        return

    attempt = row[0]
    t0 = time.time()
    db.execute("INSERT INTO inference_runs (job_id, worker) VALUES (%s, %s)", (job_id, WORKER_ID))
    label = "unknown"
    path = msg.get("path")
    if path and os.path.exists(path):
        with open(path, "rb") as f:
            label = hashlib.sha256(f.read()).hexdigest()[:8]
    time.sleep(INFER_MS / 1000.0)                      # giả lập suy luận

    with db.transaction():
        db.execute("INSERT INTO results (job_id, worker, label) VALUES (%s, %s, %s) ON CONFLICT (job_id) DO NOTHING",
                   (job_id, WORKER_ID, label))
        db.execute("UPDATE jobs SET status = 'done', finished_at = now() WHERE job_id = %s", (job_id,))
    ch.basic_ack(method.delivery_tag)                  # ack SAU khi commit
    log.info("done job=%s attempt=%s %.0fms", job_id, attempt, (time.time() - t0) * 1000)


def run_once():
    db = psycopg.connect(DATABASE_URL, autocommit=True)
    conn = rabbit_connect()
    try:
        ch = conn.channel()
        declare_queue(ch)
        ch.basic_qos(prefetch_count=1)                 # mỗi worker chỉ giữ 1 job chưa ack: chia việc công bằng
        log.info("worker %s sẵn sàng", WORKER_ID)
        for method, _props, body in ch.consume(QUEUE, inactivity_timeout=1):
            if method is None:
                if stop["flag"]:
                    break
                continue
            handle(ch, method, body, db)
            if stop["flag"]:
                break
        ch.cancel()
    finally:
        for c in (conn, db):
            try:
                c.close()
            except Exception:
                pass


def main():
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    attempt = 0
    while not stop["flag"]:
        try:
            run_once()
            attempt = 0
        except (pika.exceptions.AMQPError, psycopg.OperationalError, OSError) as exc:
            delay = backoff_delay(attempt)
            attempt += 1
            log.warning("mất kết nối (%s); thử lại sau %.1fs", type(exc).__name__, delay)
            time.sleep(delay)
    log.info("worker dừng")


if __name__ == "__main__":
    main()
