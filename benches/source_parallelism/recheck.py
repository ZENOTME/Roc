#!/usr/bin/env python3
"""Alternate baseline/final binaries for two additional four-worker rounds."""
import argparse
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("baseline", type=Path)
parser.add_argument("final", type=Path)
parser.add_argument("data", type=Path)
args = parser.parse_args()
root = Path(__file__).parent
cases = [
    ("queries", ["q51", "q47", "q57", "q67", "q23a", "q23b"], 6),
    ("groups", ["--sql", "SELECT ss_ticket_number, COUNT(*), SUM(ss_net_paid) "
     "FROM store_sales GROUP BY ss_ticket_number"], 1),
    ("limit", ["--sql", "SELECT ss_ticket_number, ss_net_paid FROM store_sales "
     "ORDER BY ss_net_paid DESC NULLS LAST, ss_ticket_number LIMIT 100"], 1),
]
for round_number, order in enumerate([["final", "baseline"], ["baseline", "final"]], 1):
    for mode in ["zero", "compile"]:
        for stage in order:
            common = [str(getattr(args, stage).resolve()), str(args.data.resolve()),
                      "--mode", mode, "--workers", "4", "--threads", "4", "--repeat", "3"]
            for label, extra, expected in cases:
                path = root / f"recheck{round_number}-{stage}-{mode}-4-{label}.txt"
                with path.open("w") as log:
                    subprocess.run(common + extra, stdout=log, stderr=subprocess.STDOUT, check=True)
                if f"passed {expected} / {expected}" not in path.read_text():
                    raise RuntimeError(f"Validation failed: {path}")
            print(f"round {round_number}: {stage}/{mode} complete", flush=True)
