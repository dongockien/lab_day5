"""Hàm gọi API dùng cho các công cụ đo (bench, check)."""
import os
import time

import requests

API = os.getenv("API_URL", "http://api:8000")


def post_job(key=None, size=20_000):
    headers = {"Idempotency-Key": key} if key else {}
    files = {"image": ("photo.jpg", os.urandom(size), "image/jpeg")}
    try:
        return requests.post(f"{API}/jobs", files=files, headers=headers, timeout=30).status_code
    except requests.RequestException:
        return 0


def stats():
    return requests.get(f"{API}/stats", timeout=30).json()


def job(job_id):
    return requests.get(f"{API}/jobs/{job_id}", timeout=30).json()


def reset():
    requests.post(f"{API}/admin/reset", timeout=60)


def wait_api(timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if requests.get(f"{API}/health", timeout=3).ok:
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


def wait_consumers(n, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        q = stats().get("queue")
        if q and q["consumers"] >= n:
            return True
        time.sleep(1)
    return False


def wait_done(n, timeout, poll=0.5):
    end = time.time() + timeout
    s = stats()
    while time.time() < end:
        s = stats()
        if s["done"] >= n:
            return s, True
        time.sleep(poll)
    return s, False
