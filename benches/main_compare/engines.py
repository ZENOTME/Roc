#!/usr/bin/env python3
"""The main_compare workloads on DuckDB and DataFusion, for reference.
Usage: engines.py ENGINE ROWS SAMPLES THREADS  (ENGINE: duckdb | duckdb-arrow | datafusion)
Prints one JSON report in the shape of bench.rs.

Input and expressions match bench.rs: 8192-row batches of id, group_key = id % 256,
value = id % 1000 - 500 (NULL when id % 17 == 0) and selector = id % 100.
- duckdb reads a resident DuckDB table and writes a temporary table: its best case.
- duckdb-arrow reads the resident Arrow batches and returns an Arrow table.
- datafusion reads resident Arrow batches split round-robin over THREADS
  partitions and collects Arrow batches.
DuckDB checks integer overflow like Roc; DataFusion's + wraps on overflow.
Prepare is SQL parsing and planning where the engine exposes it separately.
"""
import json
import statistics
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

BATCH = 8192
CASES = ["scan", "filter_project", "arithmetic_chain", "integer_division",
         "global_aggregate", "grouped_aggregate"]


def chain(division):
    e = "value"
    for k in range(1, 17):
        e = f"({e} + {k})"
        if division and k == 8:
            e = f"({e} {{div}} 3)"
    return e


SQL = {
    "scan": "SELECT id, group_key, value, selector FROM t",
    "filter_project": "SELECT id, value + 1 AS result FROM t WHERE selector > 50",
    "arithmetic_chain": f"SELECT id, {chain(False)} AS result FROM t",
    "integer_division": f"SELECT id, {chain(True)} AS result FROM t",
    "global_aggregate": "SELECT SUM(value) AS total, COUNT(value) AS count FROM t",
    "grouped_aggregate":
        "SELECT group_key, SUM(value) AS total, COUNT(value) AS count FROM t GROUP BY group_key",
}


def batches(rows):
    i = np.arange(rows, dtype=np.int64)
    table = pa.table({
        "id": i,
        "group_key": i % 256,
        "value": pa.array(i % 1000 - 500, mask=(i % 17 == 0)),
        "selector": i % 100,
    })
    return table.to_batches(max_chunksize=BATCH)


def expected(case, rows):
    i = np.arange(rows, dtype=np.int64)
    valid = i % 17 != 0
    v = i % 1000 - 500
    if case == "scan":
        return rows, None
    if case.endswith("aggregate"):
        keys = np.zeros(rows, dtype=np.int64) if case == "global_aggregate" else i % 256
        n = int(keys.max()) + 1
        sums = np.bincount(keys[valid], weights=v[valid], minlength=n).astype(np.int64)
        counts = np.bincount(keys[valid], minlength=n)
        return n, (sums, counts)
    keep = i % 100 > 50 if case == "filter_project" else np.ones(rows, dtype=bool)
    if case == "filter_project":
        result = v + 1
    elif case == "arithmetic_chain":
        result = v + 136
    else:
        # SQL integer division truncates toward zero.
        result = np.trunc((v + 36) / 3).astype(np.int64) + 100
    return int(keep.sum()), (i[keep], result[keep], valid[keep])


def check(case, rows, table):
    n, want = expected(case, rows)
    assert table.num_rows == n, (case, table.num_rows, n)
    if want is None:
        return
    if case.endswith("aggregate"):
        sums, counts = want
        if case == "grouped_aggregate":
            table = table.sort_by("group_key")
            assert table["group_key"].to_numpy().tolist() == list(range(n))
        assert table["total"].to_numpy().astype(np.int64).tolist() == sums.tolist()
        assert table["count"].to_numpy().tolist() == counts.tolist()
        return
    ids, result, valid = want
    table = table.sort_by("id")
    assert np.array_equal(table["id"].to_numpy(), ids)
    got = table["result"]
    assert np.array_equal(pc.is_valid(got).to_numpy(zero_copy_only=False), valid)
    assert np.array_equal(got.to_numpy(zero_copy_only=False)[valid].astype(np.int64), result[valid])


class DuckDB:
    def __init__(self, data, threads, arrow):
        import duckdb
        self.version = duckdb.__version__
        self.con = duckdb.connect()
        self.con.execute(f"SET threads={threads}")
        self.arrow = arrow
        source = pa.Table.from_batches(data)
        self.con.register("src", source)
        if arrow:
            self.con.execute("CREATE VIEW t AS SELECT * FROM src")
        else:
            self.con.execute("CREATE TABLE t AS SELECT * FROM src")

    def sql(self, case):
        return SQL[case].replace("{div}", "//")

    def prepare(self, case):
        return self.sql(case)

    def execute(self, sql):
        if self.arrow:
            return self.con.execute(sql).fetch_arrow_table()
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE o AS {sql}")

    def result(self, case):
        return self.con.execute(self.sql(case)).fetch_arrow_table()


class DataFusion:
    def __init__(self, data, threads):
        import datafusion
        self.version = datafusion.__version__
        config = datafusion.SessionConfig().with_target_partitions(threads).with_batch_size(BATCH)
        self.ctx = datafusion.SessionContext(config)
        self.ctx.register_record_batches("t", [data[p::threads] for p in range(threads)])

    def prepare(self, case):
        return self.ctx.sql(SQL[case].replace("{div}", "/"))

    def execute(self, frame):
        return frame.collect()

    def result(self, case):
        return pa.Table.from_batches(self.prepare(case).collect())


def main():
    engine, rows, samples, threads = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    data = batches(rows)
    if engine == "datafusion":
        e = DataFusion(data, threads)
    elif engine in ("duckdb", "duckdb-arrow"):
        e = DuckDB(data, threads, engine == "duckdb-arrow")
    else:
        raise SystemExit(f"unknown engine {engine}")
    cases = []
    for case in CASES:
        check(case, rows, e.result(case))
        prepare, execute = [], []
        for sample in range(samples + 2):
            start = time.perf_counter()
            plan = e.prepare(case)
            mid = time.perf_counter()
            e.execute(plan)
            end = time.perf_counter()
            if sample >= 2:
                prepare.append((mid - start) * 1e3)
                execute.append((end - mid) * 1e3)
        cases.append({
            "case": case,
            "prepare_ms": prepare,
            "execute_ms": execute,
            "prepare_median_ms": statistics.median(prepare),
            "execute_median_ms": statistics.median(execute),
        })
        print(f"{engine} t{threads} {case}: {statistics.median(execute):.3f}ms", file=sys.stderr)
    print(json.dumps({
        "engine": engine, "version": e.version, "rows": rows, "batch_rows": BATCH,
        "threads": threads, "samples": samples, "warmups": 2,
        "correctness": "every output row checked against an independent numpy calculation before timing",
        "cases": cases,
    }))


if __name__ == "__main__":
    main()
