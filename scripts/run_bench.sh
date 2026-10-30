#!/bin/bash
# Thí nghiệm 1: cùng 40 job, đo với 1 worker rồi 4 worker. Kết quả lưu ở results/bench.txt
source "$(dirname "$0")/lib.sh"
: > results/bench.txt
for N in 1 4; do
  echo ">>> Dựng hệ thống với $N worker..."
  start_stack "$N"
  tools app.bench --jobs 40 --workers "$N" | tee -a results/bench.txt
  echo | tee -a results/bench.txt
done
echo "Đã lưu: results/bench.txt"
