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
2026-10-15, SF1, Apple M1 Pro (machine shared with other jobs; timings are
noisy, compare engines within one run). Full per-statement report:
[`integrations/datafusion/tpcds/status.md`](../integrations/datafusion/tpcds/status.md).

Correctness: 103 / 103 in both modes with 4 workers; with 1 worker q72 used
to time out (it now takes about 1 s).

Performance, ZeroCompile, 4 workers, both engines on a 4-thread runtime
with 4 DataFusion partitions, best of 3 runs (`--repeat 3`):

| Point | Roc total | DataFusion total | Roc slower on |
| --- | ---: | ---: | --- |
| start of the phase (10-thread runtime) | 77 s | 6.5 s | 101 / 103 |
| after the commits up to `cc65ed8` | 23.4 s | 11.4 s | 88 / 103 |

DuckDB 1.5 (`tpcds/duckdb.py --threads 4`): 4.1 s over the 101 statements
it parses (q77 and q90 use reserved words as aliases).
<!-- /STATUS -->

## Performance work so far

Each item is a commit (or two) on the branch; the commit messages explain
the why.

- Runner: `--repeat`, `--profile`, `--sql`, `--explain` (pipelines with
  their prepared steps), `--threads`, `--set`; Roc phase times; DuckDB
  script. Profile with macOS `sample` while `--repeat N --profile` runs.
- Scans: DataFusion partitions decode in parallel tasks (`storage.rs`);
  scans split files into target partitions (`plan.rs`).
- Joins: indexed tables are stored grouped by hash bucket
  (`kernel/table.rs`); expansions probe a window's rows in one batch and
  stream matches straight into windows (`steps/expand.rs`); gathers and
  imports read only the rows of their domain (`Step::Import`,
  `steps/mod.rs::place_imports`).
- Translator: real row estimates from DataFusion's `StatisticsContext`;
  semi-join reduction of build sides and of probe scans by filtered
  dimension tables joined above them (`plan.rs::Reduction`).
- Aggregates: open-addressing group index (`exec/aggregate/group_index.rs`);
  128-bit sums over whole windows; partials of all workers merge as
  hash partitions in parallel CPU jobs at finish (`SinkExec::finish_on`,
  `exec::Cpu`).
- Lowering: decimal casts that cannot overflow skip their precision check.

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

Performance, largest gaps first (profile before changing anything):

1. **Large group-bys** (q23, q67, q14, q78, q97): composite fixed-width
   keys still go through the encoded-bytes path one row at a time;
   `Accumulator` keeps one ~180-byte `Group` per group for every function;
   merges decode partial arrays into cells. Next: fixed-width composite
   keys interned from whole windows, typed accumulator storage, merges of
   indexes and accumulators without Arrow arrays.
2. **Windows and sorts** (q51, q47, q57, q67): `exec/sort.rs` and
   `exec/window.rs` work on `scalar::decode` cells on one thread.
3. **Runtime pruning**: DataFusion prunes row groups of date-sorted fact
   tables with dynamic filters from join builds (q39, q21). An equivalent
   would hand key bounds of a built table to the scan of a semi-join's
   probe side; it is a storage hint and changes no semantics.
4. **Empty build sides** (q27, q68, q46): DataFusion skips the probe side;
   Roc still scans it.
5. **Compile mode overhead.** LLVM compiles every program when the graph is
   built (q2/q66 ~1.5 s). Cache fused functions by a structural key, or
   compile in the background.
6. Row-at-a-time paths: string comparisons and LIKE (`Step::Text`), table
   index builds (`kernel/table.rs`), serial table builds at `finish`.

Keep the boundary: these are kernel and execution-strategy changes; the IR
semantics and the translator stay general.
