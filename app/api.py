"""API nhận ảnh, ghi trạng thái job vào Postgres và đưa job vào RabbitMQ.

PUBLISH_MODE=direct: ghi DB rồi publish trực tiếp (ghi 2 nơi, không atomic).
PUBLISH_MODE=outbox: ghi job + outbox trong cùng 1 transaction; relay phát sang RabbitMQ sau.
"""
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool

from .common import (DATABASE_URL, PUBLISH_MODE, UPLOAD_DIR, Publisher, purge_queue,
                     queue_info, setup_logging)

log = setup_logging("api")
pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False)
publisher = Publisher()


@asynccontextmanager
async def lifespan(_app):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    pool.open(wait=True, timeout=30)
    yield
    pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"ok": True, "publish_mode": PUBLISH_MODE}


@app.post("/jobs", status_code=202)
def create_job(image: UploadFile = File(...), idempotency_key: str | None = Header(default=None)):
    # Idempotency-Key (nếu có) được dùng làm job_id. API KHÔNG tự loại trùng để lab kiểm tra phía consumer.
    job_id = idempotency_key or uuid.uuid4().hex
    data = image.file.read()
    path = os.path.join(UPLOAD_DIR, f"{job_id}.bin")
    with open(path, "wb") as f:
        f.write(data)
    msg = {"job_id": job_id, "path": path, "bytes": len(data)}

    if PUBLISH_MODE == "outbox":
        with pool.connection() as conn:                      # 1 transaction: cả hai cùng ghi hoặc cùng không
            conn.execute("INSERT INTO jobs (job_id, status, image_bytes) VALUES (%s, 'queued', %s) "
                         "ON CONFLICT (job_id) DO NOTHING", (job_id, len(data)))
            conn.execute("INSERT INTO outbox (job_id, payload) VALUES (%s, %s)", (job_id, Json(msg)))
        return {"job_id": job_id, "mode": "outbox"}

    with pool.connection() as conn:
        conn.execute("INSERT INTO jobs (job_id, status, image_bytes) VALUES (%s, 'queued', %s) "
                     "ON CONFLICT (job_id) DO NOTHING", (job_id, len(data)))
    try:
        publisher.publish(msg)
    except Exception as exc:
        log.warning("publish failed job=%s err=%s", job_id, exc)
        raise HTTPException(status_code=503, detail="queue unavailable")
    return {"job_id": job_id, "mode": "direct"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with pool.connection() as conn:
        row = conn.execute("SELECT status, worker, attempts, created_at, finished_at FROM jobs WHERE job_id = %s",
                           (job_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="not found")
        results = conn.execute("SELECT count(*) FROM results WHERE job_id = %s", (job_id,)).fetchone()[0]
        runs = conn.execute("SELECT count(*) FROM inference_runs WHERE job_id = %s", (job_id,)).fetchone()[0]
    return {"job_id": job_id, "status": row[0], "worker": row[1], "attempts": row[2],
            "result_rows": results, "inference_runs": runs}


@app.get("/stats")
def stats():
    with pool.connection() as conn:
        by_status = dict(conn.execute("SELECT status, count(*) FROM jobs GROUP BY status").fetchall())
        results_rows = conn.execute("SELECT count(*) FROM results").fetchone()[0]
        runs = conn.execute("SELECT count(*) FROM inference_runs").fetchone()[0]
        redelivered, max_attempts = conn.execute(
            "SELECT count(*) FILTER (WHERE attempts > 1), coalesce(max(attempts), 0) FROM jobs").fetchone()
        outbox_pending = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
        pct, avg, mx = conn.execute(
            "SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY d), avg(d), max(d) "
            "FROM (SELECT extract(epoch FROM finished_at - created_at)::float8 AS d "
            "      FROM jobs WHERE status = 'done') t").fetchone()
    return {
        "jobs": by_status,
        "done": by_status.get("done", 0),
        "queued": by_status.get("queued", 0),
        "results_rows": results_rows,
        "inference_runs": runs,
        "redelivered_jobs": redelivered,
        "max_attempts": max_attempts,
        "outbox_pending": outbox_pending,
        "latency": None if pct is None else {"p50": pct[0], "p95": pct[1], "p99": pct[2], "avg": avg, "max": mx},
        "queue": queue_info(),
    }


@app.post("/admin/reset")
def reset():
    """Chỉ dùng cho lab: xóa dữ liệu và làm rỗng hàng đợi trước mỗi lần đo."""
    with pool.connection() as conn:
        conn.execute("TRUNCATE jobs, results, inference_runs, outbox RESTART IDENTITY")
    purge_queue()
    return {"reset": True}
