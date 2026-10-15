#!/usr/bin/env python3
"""Collect the TPC-DS harness's best-of-three reports as a comparable CSV."""
import csv
import re
from pathlib import Path

root = Path(__file__).parent
rows = []
for path in sorted(root.glob("*.txt")):
    match = re.fullmatch(r"(.+)-(zero|compile)-([14])-(queries|groups|limit)\.txt", path.name)
    if not match:
        continue
    stage, mode, workers, case = match.groups()
    for line in path.read_text().splitlines():
        if not line.startswith("| "):
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        query, status, count, roc, df, times, finish, detail = cells
        if status != "passed":
            raise RuntimeError(f"Unvalidated measurement: {path.name}: {line}")
        translate, build, execute = times.split(" / ")
        rows.append(dict(stage=stage, mode=mode, workers=workers,
                         query=case if query == "sql" else query, status=status, rows=count,
                         roc_ms=roc, datafusion_ms=df, translate_ms=translate,
                         build_ms=build, execute_ms=execute, sink_finish_ms=finish))
with (root / "results.csv").open("w") as out:
    writer = csv.DictWriter(out, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
print(f"Wrote {len(rows)} validated measurements")
