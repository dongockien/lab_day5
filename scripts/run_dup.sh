#!/bin/bash
# Thí nghiệm 3: gửi trùng một job (cùng Idempotency-Key), kiểm tra chỉ xử lý 1 lần. Kết quả: results/dup.txt
source "$(dirname "$0")/lib.sh"
start_stack 4
tools app.check dup | tee results/dup.txt
echo "Đã lưu: results/dup.txt"
