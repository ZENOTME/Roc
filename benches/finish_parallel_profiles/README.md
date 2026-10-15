# Parallel final finish evidence

Results for section 12 of [the design](../../design/sink-parallelism.md).
Production sources contain no stage instrumentation or experiment switches.

- `stages.csv`: for each query, pipeline and stage, sum repeated spans within a
  run, then report the median/min/max of warm runs. `*_job`, `local.*`,
  `aggregate.intern`, `aggregate.accumulator`, `aggregate.group_columns` and
  `aggregate.drop_index` are **sums of worker elapsed times**, not query wall time.
  `*_wall` and `sink_finish.*` measure the coordinator's wait, including queueing.
  Nested spans are inclusive; different pipelines may overlap.
- `intervals.csv.gz`: parsed raw intervals, row counts, run/pipeline/thread IDs.
- `q51.trace.gz`, `q67.trace.gz`, `q23a.trace.gz`, `q23b.trace.gz`: original
  temporary-instrumented logs, six runs each; summaries exclude run 1.
- `q23-serial.trace.gz`: Table timing collected before implementation, four
  q23a runs followed by four q23b runs. Exclude the first run of each query.
- `single_worker_q23.csv`: uninstrumented, alternating serial/parallel comparisons,
  best of three, two rounds per mode. These supplement the full matrix; they do
  not replace its less favorable measurements.
- `single_partition_experiment.csv`: rejected one-worker experiment that merged
  inline when every grouping set had one partition. No stable benefit was found;
  the final implementation has no such fallback.
- `metadata.json`: environment, method, source hashes and validation summary.
- `instrumentation.patch`: instrumentation against the section 12 source. Checked
  with `git apply --check`; apply only to a disposable copy.

Full uninstrumented best-of-three results are the `finish_parallel` rows in
[`sink_parallelism.csv`](../sink_parallelism.csv). All 28 configurations match
DataFusion. Full SF1 correctness results for both modes are in
[`finish_parallel_validation.csv`](../finish_parallel_validation.csv).
Profile repetitions themselves do not validate results.

```sh
# Run from the section 12 source tree.
profile_root=$(mktemp -d /tmp/roc-finish-profile.XXXXXX)
git ls-files -z | tar --null -T - -cf - | tar -xf - -C "$profile_root"
git -C "$profile_root" apply "$PWD/benches/finish_parallel_profiles/instrumentation.patch"
CARGO_TARGET_DIR="$PWD/target" cargo build --locked --release \
  -p roc-datafusion --example tpcds --manifest-path "$profile_root/Cargo.toml"
cp target/release/examples/tpcds "$profile_root/tpcds"

# Repeat separately for q51, q67, q23a and q23b.
"$profile_root/tpcds" <sf1-dir> --mode zero --workers 4 --threads 4 \
  --repeat 6 --profile q51 > "$profile_root/q51.trace" 2>&1

# Restore the normal binary after sharing the Cargo target directory.
cargo build --locked --release -p roc-datafusion --example tpcds

# Uninstrumented full validation; repeat with --mode compile.
target/release/examples/tpcds <sf1-dir> --mode zero --workers 4 --threads 4
```

Do not overlap timing runs with compilation, tests, sampling or other queries.
Fine-grained logging perturbs timing: instrumented warm medians are 219.9 ms
for q51 and 524.9 ms for q67, versus uninstrumented best-of-three 194.3/471.2 ms.
Do not combine these into one elapsed-time equation.
