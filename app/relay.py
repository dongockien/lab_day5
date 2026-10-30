"""Outbox relay: đọc sự kiện chưa phát trong bảng outbox và phát sang RabbitMQ.

Phát trước, đánh dấu published_at sau, trong cùng một transaction. Nếu relay chết giữa chừng,
sự kiện sẽ được phát lại (at-least-once), nên consumer phải idempotent.
"""
import signal
import time

import pika
import psycopg

from .common import DATABASE_URL, Publisher, backoff_delay, setup_logging

POLL_SECONDS = 0.2
log = setup_logging("relay")
stop = {"flag": False}


def run_once():
    db = psycopg.connect(DATABASE_URL, autocommit=True)
    pub = Publisher()
    try:
        while not stop["flag"]:
            with db.transaction():
                rows = db.execute("SELECT id, payload FROM outbox WHERE published_at IS NULL "
                                  "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED").fetchall()
                for _id, payload in rows:
                    pub.publish(payload)           # lỗi ở đây -> rollback, sự kiện vẫn nằm trong outbox
                if rows:
                    db.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)", ([r[0] for r in rows],))
            if rows:
                log.info("đã phát %d sự kiện", len(rows))
            else:
                time.sleep(POLL_SECONDS)
    finally:
        db.close()


def main():
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    attempt = 0
    while not stop["flag"]:
        try:
            run_once()
        except (pika.exceptions.AMQPError, psycopg.OperationalError, OSError) as exc:
            delay = backoff_delay(attempt)
            attempt += 1
            log.warning("relay lỗi (%s); thử lại sau %.1fs", type(exc).__name__, delay)
            time.sleep(delay)
        else:
            attempt = 0


if __name__ == "__main__":
    main()
