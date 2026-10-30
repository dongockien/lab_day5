"""Đo thông lượng và độ trễ khi gửi một loạt job: python -m app.bench --jobs 40 --workers 4"""
import argparse
import time
from concurrent.futures import ThreadPoolExecutor

from . import client
from .common import INFER_MS


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=40)
    ap.add_argument("--workers", type=int, default=1, help="số worker đang chạy (để chờ đủ consumer)")
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--timeout", type=int, default=300)
    a = ap.parse_args()

    if not client.wait_api():
        raise SystemExit("API chưa sẵn sàng")
    if not client.wait_consumers(a.workers):
        raise SystemExit(f"chưa đủ {a.workers} worker kết nối RabbitMQ")
    client.reset()

    t0 = time.time()
    with ThreadPoolExecutor(a.concurrency) as ex:
        codes = list(ex.map(lambda _: client.post_job(), range(a.jobs)))
    t_sent = time.time() - t0
    ok = sum(1 for c in codes if c == 202)
    s, finished = client.wait_done(ok, a.timeout)
    total = time.time() - t0

    lat = s["latency"] or {}
    theory = a.workers / (INFER_MS / 1000.0)
    print(f"=== bench: workers={a.workers} jobs={a.jobs} infer={INFER_MS}ms ===")
    print(f"accepted_jobs         {ok}/{a.jobs}   (gửi xong sau {t_sent:.1f}s)")
    print(f"completed_jobs        {s['done']}/{ok}" + ("" if finished else "   (HẾT THỜI GIAN CHỜ)"))
    print(f"total_time_s          {total:.1f}")
    print(f"throughput_jobs_s     {s['done'] / total:.2f}   (lý thuyết tối đa {theory:.2f})")
    print(f"latency_avg_s         {lat.get('avg', 0):.2f}")
    print(f"latency_p50_s         {lat.get('p50', 0):.2f}")
    print(f"latency_p95_s         {lat.get('p95', 0):.2f}")
    print(f"latency_p99_s         {lat.get('p99', 0):.2f}")
    print(f"latency_max_s         {lat.get('max', 0):.2f}")


if __name__ == "__main__":
    main()
