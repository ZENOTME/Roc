#!/usr/bin/env bash
set -euo pipefail
cd /home/ze/benchmarks/roc-modes-20261013/repo
export RUSTUP_TOOLCHAIN=1.95.0
export CARGO_BUILD_JOBS=4
export HTTPS_PROXY=http://127.0.0.1:7897
export HTTP_PROXY=http://127.0.0.1:7897
unset ROC_BENCH_CASES
taskset -c 0,2,4,6 python3 benches/main_compare/run.py --output target/mylinux-modes --baseline 62f8bb97a87a09be8eea4d19ea56c9fa2ced905a --rows 4000000 --samples 10 --replicas 3
