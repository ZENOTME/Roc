#!/usr/bin/env python3
# Copyright 2026 The Roc Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Times the corpus on DuckDB over the same Parquet files, for comparison
with the runner (`examples/tpcds.rs`).

    duckdb.py <data-dir> [--threads N] [--repeat N] [--report FILE] [q3 q42 ...]

Statements are split and named as the runner does (`q14a`, `q14b`, ...).
Prints one markdown row per statement with the best of `--repeat` times;
needs the `duckdb` CLI.
"""
import argparse
import pathlib
import re
import subprocess
import sys

QUERIES = pathlib.Path(__file__).parent / "queries"


def statements():
    for q in range(1, 100):
        text = (QUERIES / f"q{q}.sql").read_text()
        parts = [p for p in (strip_comments(p) for p in text.split(";")) if p.strip()]
        for i, sql in enumerate(parts):
            name = f"q{q}" if len(parts) == 1 else f"q{q}{chr(ord('a') + i)}"
            yield name, sql.strip()


def strip_comments(sql):
    return "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--report")
    parser.add_argument("only", nargs="*")
    args = parser.parse_args()
    data = pathlib.Path(args.data)
    views = "".join(
        f"create view {p.stem} as select * from read_parquet('{p}');\n"
        for p in sorted(data.glob("*.parquet"))
    )
    lines = ["| statement | DuckDB ms | detail |", "| --- | ---: | --- |"]
    total = 0.0
    for name, sql in statements():
        if args.only and name not in args.only:
            continue
        script = f"{views}set threads = {args.threads};\n.output /dev/null\n.timer on\n"
        script += f"{sql};\n" * args.repeat
        try:
            run = subprocess.run(
                ["duckdb"], input=script, capture_output=True, text=True, timeout=args.timeout
            )
            times = [float(t) for t in re.findall(r"Run Time \(s\): real ([0-9.]+)", run.stdout + run.stderr)]
            error = run.stderr.strip().splitlines()[:1] if run.returncode else []
        except subprocess.TimeoutExpired:
            times, error = [], [f"timed out after {args.timeout} s"]
        if times and not error:
            best = min(times) * 1e3
            total += best
            line = f"| {name} | {best:.1f} |  |"
        else:
            line = f"| {name} |  | {' '.join(error).replace('|', '/')[:200]} |"
        print(line, flush=True)
        lines.append(line)
    print(f"total {total:.1f} ms")
    if args.report:
        header = f"DuckDB, {args.threads} threads, best of {args.repeat}: total {total:.1f} ms\n\n"
        pathlib.Path(args.report).write_text(header + "\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
