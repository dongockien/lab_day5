"""Cấu hình và hàm dùng chung cho api, worker, relay và các công cụ đo."""
import json
import logging
import os
import random
import threading

import pika

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secret@localhost:5432/lab")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE = os.getenv("QUEUE", "jobs")
INFER_MS = int(os.getenv("INFER_MS", "800"))              # thời gian "suy luận" giả lập
LEASE_SECONDS = float(os.getenv("LEASE_SECONDS", "5"))     # phải lớn hơn thời gian xử lý tối đa của 1 job
REQUEUE_DELAY = float(os.getenv("REQUEUE_DELAY", "0.5"))   # nghỉ trước khi trả message về hàng đợi
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/data/uploads")
PUBLISH_MODE = os.getenv("PUBLISH_MODE", "direct")         # direct | outbox


def setup_logging(name):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)   # tắt log kết nối chi tiết của pika
    return logging.getLogger(name)


def backoff_delay(attempt, base=0.5, cap=10.0):
    """Exponential backoff + full jitter: ngẫu nhiên trong [0, min(cap, base * 2^attempt)]."""
    return random.uniform(0, min(cap, base * (2 ** attempt)))


def rabbit_connect():
    params = pika.URLParameters(RABBIT_URL)
    params.heartbeat = 30
    params.socket_timeout = 3
    params.connection_attempts = 1
    params.blocked_connection_timeout = 30
    return pika.BlockingConnection(params)


def declare_queue(ch):
    ch.queue_declare(queue=QUEUE, durable=True)   # durable: hàng đợi sống sau khi broker khởi động lại


def queue_info():
    """Trả về (số message đang chờ, số consumer) hoặc None nếu không kết nối được RabbitMQ."""
    try:
        conn = rabbit_connect()
        try:
            ch = conn.channel()
            q = ch.queue_declare(queue=QUEUE, durable=True)
            return {"messages": q.method.message_count, "consumers": q.method.consumer_count}
        finally:
            conn.close()
    except Exception:
        return None


def purge_queue():
    try:
        conn = rabbit_connect()
        try:
            ch = conn.channel()
            declare_queue(ch)
            ch.queue_purge(QUEUE)
        finally:
            conn.close()
    except Exception:
        pass


class Publisher:
    """Phát message persistent kèm publisher confirm. Kết nối lại một lần nếu kết nối cũ đã chết."""

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._ch = None

    def _ensure(self):
        if self._conn is None or self._conn.is_closed or self._ch is None or self._ch.is_closed:
            self._conn = rabbit_connect()
            self._ch = self._conn.channel()
            self._ch.confirm_delivery()          # basic_publish chờ broker xác nhận đã nhận
            declare_queue(self._ch)

    def _close(self):
        try:
            if self._conn is not None and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        self._ch = None

    def publish(self, body):
        with self._lock:
            for attempt in (1, 2):
                try:
                    self._ensure()
                    self._ch.basic_publish(
                        exchange="",
                        routing_key=QUEUE,
                        body=json.dumps(body).encode(),
                        properties=pika.BasicProperties(
                            delivery_mode=2,               # persistent: ghi xuống đĩa
                            content_type="application/json",
                            message_id=body["job_id"],
                        ),
                        mandatory=True,
                    )
                    return
                except (pika.exceptions.AMQPError, OSError):
                    self._close()
                    if attempt == 2:
                        raise
