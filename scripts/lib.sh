#!/bin/bash
# Hàm dùng chung cho các script chạy thí nghiệm.
export MSYS_NO_PATHCONV=1          # Git Bash/Windows: không tự đổi đường dẫn như /tmp
cd "$(dirname "$0")/.." || exit 1
mkdir -p results

# Chạy công cụ đo trong container tạm cùng mạng Docker (--no-deps: không đụng tới các service khác)
tools() { docker compose run --rm --no-deps -T tools python -m "$@"; }

# Dựng toàn bộ hệ thống với số worker mong muốn và chờ các service khỏe
start_stack() {
  local workers="${1:-1}"
  docker compose up -d --build --wait --scale worker="$workers" postgres rabbitmq api relay worker
}
