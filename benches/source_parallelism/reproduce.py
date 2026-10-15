#!/usr/bin/env python3
"""Run the validated source-parallelism matrix, without overlapping workloads."""
import argparse
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("binary", type=Path)
parser.add_argument("data", type=Path)
parser.add_argument("stage")
parser.add_argument("--output", type=Path, default=Path(__file__).parent)
parser.add_argument("--repeat", type=int, default=3)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
cases = [
    ("queries", ["q51", "q47", "q57", "q67", "q23a", "q23b"], 6),
    ("groups", ["--sql", "SELECT ss_ticket_number, COUNT(*), SUM(ss_net_paid) "
     "FROM store_sales GROUP BY ss_ticket_number"], 1),
    ("limit", ["--sql", "SELECT ss_ticket_number, ss_net_paid FROM store_sales "
     "ORDER BY ss_net_paid DESC NULLS LAST, ss_ticket_number LIMIT 100"], 1),
]
for mode in ["zero", "compile"]:
    for workers in [1, 4]:
        common = [str(args.binary.resolve()), str(args.data.resolve()), "--mode", mode,
                  "--workers", str(workers), "--threads", str(workers),
                  "--repeat", str(args.repeat)]
        for label, extra, expected in cases:
            path = args.output / f"{args.stage}-{mode}-{workers}-{label}.txt"
            with path.open("w") as log:
                subprocess.run(common + extra, stdout=log, stderr=subprocess.STDOUT, check=True)
            if f"passed {expected} / {expected}" not in path.read_text():
                raise RuntimeError(f"Result validation failed: {path}")
            print(path.name, flush=True)
