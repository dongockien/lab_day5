"""Kiểm tra độ bền: python -m app.check <kill|dup|outbox-send|outbox-wait> [tùy chọn]"""
import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from . import client


def cmd_kill(a):
    client.wait_api()
    client.wait_consumers(1)
    client.reset()
    with ThreadPoolExecutor(10) as ex:
        codes = list(ex.map(lambda _: client.post_job(), range(a.jobs)))
    ok = sum(1 for c in codes if c == 202)
    print(f"SENT {ok} jobs - đang xử lý, có thể kill một worker ngay bây giờ", flush=True)
    s, finished = client.wait_done(ok, a.timeout)
    lost = ok - s["results_rows"]
    print("=== kết quả kiểm tra kill worker ===")
    print(f"submitted_jobs        {ok}")
    print(f"results_rows          {s['results_rows']}")
    print(f"lost_jobs             {lost}")
    print(f"redelivered_jobs      {s['redelivered_jobs']}   (job phải xử lý lại vì worker chết, attempts > 1)")
    print(f"max_attempts          {s['max_attempts']}")
    print(f"inference_runs        {s['inference_runs']}   (lớn hơn submitted nghĩa là có việc bị làm lại)")
    print(f"latency_max_s         {(s['latency'] or {}).get('max', 0):.2f}")
    print("RESULT                " + ("PASS (không mất job)" if lost == 0 and finished else "FAIL"))
    return 0 if lost == 0 and finished else 1


def cmd_dup(a):
    client.wait_api()
    client.wait_consumers(1)
    client.reset()
    key = f"dup-{uuid.uuid4().hex[:8]}"
    with ThreadPoolExecutor(2) as ex:                 # 2 request cùng Idempotency-Key, gửi đồng thời
        codes = list(ex.map(lambda _: client.post_job(key), range(2)))
    print(f"job_id={key}  gửi đồng thời 2 request -> HTTP {codes}", flush=True)
    end = time.time() + a.timeout
    while time.time() < end and client.job(key)["status"] != "done":
        time.sleep(0.3)
    time.sleep(2)                                      # chờ bản giao trùng (nếu có) được worker xử lý
    j1 = client.job(key)
    code3 = client.post_job(key)                       # gửi lần 3 sau khi job đã xong
    time.sleep(3)
    j2 = client.job(key)
    print("=== kết quả kiểm tra gửi trùng ===")
    print(f"sau 2 request đồng thời: result_rows={j1['result_rows']} inference_runs={j1['inference_runs']} attempts={j1['attempts']}")
    print(f"sau request thứ 3 (HTTP {code3}) khi job đã xong: result_rows={j2['result_rows']} inference_runs={j2['inference_runs']} attempts={j2['attempts']}")
    good = j2["result_rows"] == 1 and j2["inference_runs"] == 1
    print("RESULT                " + ("PASS (job chỉ xử lý 1 lần)" if good else "FAIL"))
    return 0 if good else 1


def cmd_outbox_send(a):
    client.wait_api()
    client.reset()
    codes = [client.post_job() for _ in range(a.jobs)]
    print(f"HTTP status của {a.jobs} request: {codes}")
    print(f"accepted(202)={codes.count(202)}  rejected(503)={codes.count(503)}  khác={len(codes) - codes.count(202) - codes.count(503)}")
    return 0


def cmd_outbox_wait(a):
    s, _ = client.wait_done(a.expect, a.timeout)
    print("=== trạng thái sau khi RabbitMQ chạy lại ===")
    print(f"expected_jobs         {a.expect}")
    print(f"results_rows          {s['results_rows']}")
    print(f"jobs_stuck_queued     {s['queued']}   (job có trong DB nhưng chưa bao giờ vào RabbitMQ)")
    print(f"outbox_pending        {s['outbox_pending']}")
    print(f"lost_jobs             {a.expect - s['results_rows']}")
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("kill"); k.add_argument("--jobs", type=int, default=40); k.add_argument("--timeout", type=int, default=120)
    d = sub.add_parser("dup"); d.add_argument("--timeout", type=int, default=30)
    o = sub.add_parser("outbox-send"); o.add_argument("--jobs", type=int, default=5)
    w = sub.add_parser("outbox-wait"); w.add_argument("--expect", type=int, default=5); w.add_argument("--timeout", type=int, default=40)
    a = ap.parse_args()
    sys.exit({"kill": cmd_kill, "dup": cmd_dup, "outbox-send": cmd_outbox_send, "outbox-wait": cmd_outbox_wait}[a.cmd](a))


if __name__ == "__main__":
    main()
