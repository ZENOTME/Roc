# TPC-DS in Roc: progress and handoff

Tracks [issue #40](https://github.com/ZENOTME/Roc/issues/40) (sub-issues #41–#45):
run all 103 statements of the pinned TPC-DS corpus through Roc, with
DataFusion as the frontend, then make Roc faster than DuckDB and DataFusion.
Update this file whenever a milestone lands; it is the entry point for
whoever continues.

## Goal and constraints

1. Every statement of the corpus plans in DataFusion, translates into a Roc
   operator tree and runs through Roc with results equal to DataFusion's, in
   both execution modes and with one and several workers.
2. Then performance: `ZeroCompile` should try to beat DuckDB and DataFusion,
   `Compile` should beat both across the board.
3. **Framework boundary.** All relational semantics are expressed in the
   kernel IR (`src/kernel/model.rs`: variables, row domains, `ScalarKernel`,
   `DomainKernel` Filter/Expand/Reduce, states) and the existing
   source → program → sink pipeline model. Do not add side abstractions that
   move semantics outside the IR (an earlier attempt added a `StreamOperator`
   chain between programs; it was rejected and reverted). Kernels may be
   specialized freely; the framework and the translator must not special-case
   TPC-DS queries. Completeness first, no performance hacks across
   boundaries.
4. Small, reviewable commits.

## Environment

- LLVM 19 is required: `LLVM_SYS_191_PREFIX=/opt/homebrew/opt/llvm@19`.
- Corpus: `integrations/datafusion/tpcds/queries/` (DataFusion 55.1.0,
  99 files, 103 statements; `q14a`/`q14b` name multi-statement files).
- Data: `integrations/datafusion/tpcds/setup.sh [data-dir] [sf]` generates
  Parquet with DuckDB's `dsdgen` (needs the `duckdb` CLI and network for the
  `tpcds` extension). SF1 lives in `../roc-data/tpcds/sf1` next to the
  repository (370 MB, not committed).
- Run the corpus:

  ```sh
  cargo run --release -p roc-datafusion --example tpcds -- <data-dir>/sf1 \
      [--mode compile|zero] [--workers N] [--timeout S] [--report FILE] [q3 q42 ...]
  ```

  Prints one row per statement: the stage it reached (`plan`, `translate`,
  `execute`, `validate`, `passed`), rows, Roc and DataFusion times, and the
  first line of the diagnostic.
- Audit what the optimized plans use:
  `cargo run --release -p roc-datafusion --example tpcds_audit -- <data-dir>/sf1 [q..]`
  (with query names it also prints their plans).

## Architecture as built

| Capability | Where | How |
| --- | --- | --- |
| Decimal128 | `kernel/decimal.rs`, codegen `Ty::I128` | Int128 unscaled values; Arrow's result scales, rounding half away from zero, precision checks |
| Utf8View / BinaryView | `kernel/builder.rs`, `steps/run.rs`, `steps/output.rs` | byte pointer + length bindings like other strings |
| Joins (inner, left, semi, anti, mark, product) | `operator/join.rs` | build side → `TableSinkExec` in its own pipeline; probe side `DomainKernel::Expand` (`ExpandKind::Lookup` / `Product`) + `TableGather`; semi/anti/mark via `Reduce(Any)` |
| Expand/Reduce execution | `kernel/plan.rs` (row spaces), `kernel/steps/{mod,expand}.rs` | one step block per row space; expansions run their block over windows of (input row, table row) pairs |
| Full join | `integrations/datafusion/src/plan.rs` | left join ∪ right anti join with NULL left columns, both sides shared through `SpoolOperator` |
| Sort, limit, union, single row, spool, window | `operator/materialize.rs`, `exec/{buffer,sort,window}.rs` | child pipeline sinks into a buffer (optionally transformed when its input ends); next pipeline reads it; sorted results make the reading pipeline `ordered` (one execution instance) |
| Grouping sets / ROLLUP | `operator/aggregate.rs`, `exec/aggregate/mod.rs` | one group lookup + accumulators per set; grouping id column |
| Aggregates | `expr/agg/` | COUNT[DISTINCT], SUM/AVG (decimal exact), MIN, MAX, COVAR_POP, STDDEV_SAMP |
| Scalar functions | `expr/scalar/function.rs`, `like.rs`, `kernel/text.rs` | ABS, ROUND, SUBSTR, UPPER, `\|\|`, LIKE, `&`, `>>`, IN lists (as ORs), date ± days |
| Scalar subqueries | translator | `SingleRowOperator` (NULL for zero rows, error for several) joined as a product |
| DataFusion translation | `integrations/datafusion/src/plan.rs` | one Roc column per schema field per node |
| Validation | `integrations/datafusion/src/run.rs` | same plan executed by DataFusion; types, then rows as sequence or multiset (floats with 1e-9 relative tolerance) |

## Status

Commit history on the branch tells the order of work. Update this section
and `tpcds/status.md` from the runner output after each milestone.

<!-- STATUS -->
2026-10-15, SF1, Apple M1 Pro. Full per-statement report:
[`integrations/datafusion/tpcds/status.md`](../integrations/datafusion/tpcds/status.md).

| Mode | Workers | Passed | Roc total | DataFusion total |
| --- | ---: | --- | ---: | ---: |
| Compile | 4 | 103 / 103 | 96.5 s | 6.5 s |
| ZeroCompile | 4 | 103 / 103 | 82.0 s | 6.5 s |
| Compile | 1 | 102 / 103 (q72: 120 s timeout) | 58.9 s | 5.6 s |
| ZeroCompile | 1 | 102 / 103 (q72: 120 s timeout) | 43.2 s | 5.6 s |

Correctness is complete for the corpus at SF1 (q72 is correct with 4
workers, 56 s). Performance has not been worked on yet: Roc is about 10x
slower than DataFusion in total, slower with 4 workers than with 1, and the
worst statements are 40–75x slower (q72, q2, q36, q43, q66, q27, q21).
<!-- /STATUS -->

## Known gaps and next steps

Correctness:

- Validation compares row sequences, then multisets; for a multiset match
  it checks Roc's rows against the plan's sort keys when they are output
  columns (`run::output_order`). Sorts by expressions are not checked.
- Correlated scalar subqueries are not translated (the optimizer
  decorrelates the corpus's; one that remains fails loudly).
- Window frames other than whole partition, `ROWS ... CURRENT ROW` and
  `RANGE ... CURRENT ROW` are unsupported; `LEFT JOIN` with a residual
  filter is unsupported (`JoinOperator::try_new`).
- Not yet covered: larger scale factors; an automated test that runs the
  corpus (it needs the generated data, so it is an example today).

Performance (the next phase; profile before changing anything):

1. **Multi-worker slowdown.** 4 workers are slower than 1. Suspects:
   materializing operators concatenate and sort on the coordinator thread
   (`exec/buffer.rs`, `exec/sort.rs`, `exec/window.rs`); `TableSinkExec`
   builds join tables serially at `finish`; DataFusion scan partitions vs.
   worker count (`storage.rs`).
2. **Compile mode overhead.** LLVM compiles every program of every
   pipeline when the graph is built (q2: 1.7 s Compile vs 0.1 s
   ZeroCompile). Options inside the framework: cache fused functions by a
   structural key of the fused group, compile lazily or in the background
   and run zero-compile kernels until code is ready, or skip fusion for
   pipelines with little input.
3. **q72** (56 s vs 0.76 s): a join with a large fan-out and residual
   filter; expansions call `Table::find` per row and gather per row
   (`kernel/steps/expand.rs`) — vectorize probe and gather kernels.
4. Row-at-a-time paths that should become kernels: string comparisons and
   LIKE (`Step::Text`), group lookups with multiple or string keys
   (`GroupIndex::intern_row`), table index builds (`kernel/table.rs`),
   and windows/sorts over `scalar::decode` cells.

Keep the boundary: these are kernel and execution-strategy changes; the IR
semantics and the translator stay general.
