#!/bin/bash
# Thí nghiệm 4 (mở rộng): RabbitMQ chết lúc API nhận job. So sánh direct và outbox. Kết quả: results/outbox.txt
source "$(dirname "$0")/lib.sh"
: > results/outbox.txt

run_round() {
  local mode="$1"
  echo "##### PUBLISH_MODE=$mode" | tee -a results/outbox.txt
  PUBLISH_MODE="$mode" docker compose up -d --build --wait --force-recreate --scale worker=2 postgres rabbitmq api relay worker
  echo ">>> Dừng RabbitMQ" | tee -a results/outbox.txt
  docker compose stop rabbitmq > /dev/null
  tools app.check outbox-send --jobs 5 | tee -a results/outbox.txt
  echo ">>> Bật lại RabbitMQ" | tee -a results/outbox.txt
  docker compose start rabbitmq > /dev/null
  until docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping > /dev/null 2>&1; do sleep 2; done
  tools app.check outbox-wait --expect 5 --timeout 60 | tee -a results/outbox.txt
  echo | tee -a results/outbox.txt
}

run_round direct
run_round outbox
echo "Đã lưu: results/outbox.txt"
