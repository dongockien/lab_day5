#!/bin/bash
# Thí nghiệm 2: kill một worker giữa chừng, kiểm tra không mất job. Kết quả: results/kill.txt
source "$(dirname "$0")/lib.sh"
start_stack 4
tools app.check kill --jobs 40 | tee results/kill.txt | while IFS= read -r line; do
  echo "$line"
  if [[ "$line" == SENT* ]]; then
    sleep 2
    CID=$(docker compose ps -q worker | head -n 1 | tr -d '\r')
    echo ">>> docker kill $CID (SIGKILL, mô phỏng worker chết đột ngột)"
    docker kill "$CID" > /dev/null
  fi
done
echo "Đã lưu: results/kill.txt"
