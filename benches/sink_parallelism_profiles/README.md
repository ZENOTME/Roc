# Sink parallelism hotspot evidence

These are diagnostic artifacts for [q51/q67 analysis](../../design/sink-parallelism-hotspots.md).
The production sources contain none of this instrumentation or the experimental switches.

- `metadata.json`: environment, method, variants, uninstrumented timings.
- `traces.csv.gz`: raw stage intervals, row counts, query run, pipeline and thread IDs.
  Runs start at 1 within each variant/query process; run 1 is excluded from summaries.
  Nested spans are inclusive. Before-version stage thread-local pipeline IDs can become
  stale across async resumption; the outer sink span IDs are authoritative. Attribute
  nested before-version stages by containment within the matching sink interval.
- `*.sample.txt.gz`: original macOS `sample` output for uninstrumented binaries.
- `sample-summary.txt`: self samples restricted to Window/Aggregate coordinator finish
  call trees. Counts are not query wall-time percentages.
- `validation.txt`: separate DataFusion comparisons for all three experimental variants
  in both execution modes; 12/12 passed. The profile repetitions themselves do not validate.
- `after.patch`: temporary timing instrumentation plus two optional experimental switches.
- `before.patch`: timing instrumentation for baseline commit
  `37e69782efb87a6b0e8d0b415170b0b8d25e0695`.

The after patch applies to the section 11 **serial LocalSink snapshot**, before
section 12 introduced parallel final finish. It does not apply to the later
working tree. Preserve/use that source snapshot for the commands below.
Both patches passed `git apply --check` against their respective source snapshots.
Use a disposable source copy, not the working tree:

```sh
profile_root=$(mktemp -d /tmp/roc-sink-profile.XXXXXX)
# Run from the section 11 source snapshot, not the section 12 working tree.
git ls-files -z | tar --null -T - -cf - | tar -xf - -C "$profile_root"
git -C "$profile_root" apply "$PWD/benches/sink_parallelism_profiles/after.patch"
CARGO_TARGET_DIR="$PWD/target" cargo build --locked --release \
  -p roc-datafusion --example tpcds --manifest-path "$profile_root/Cargo.toml"
cp target/release/examples/tpcds "$profile_root/tpcds"

# Run each query separately, redirecting stdout and stderr to its own trace.
"$profile_root/tpcds" <sf1-dir> --mode zero --workers 4 --threads 4 \
  --repeat 6 --profile q51 > "$profile_root/q51.trace" 2>&1

# Other variants, applied to the same command:
# ROC_PROFILE_DROP_ON_WORKER=1
# ROC_PROFILE_COALESCE=16
# Both environment variables together
```

To reproduce the baseline, populate a separate empty directory with `git archive` of
the baseline commit, apply `before.patch`, and build with the same command. Its runtime
already has four threads; omit `--threads`. Baseline and after use the same Cargo.lock.

For a correctness run omit `--profile` and use `--repeat 1`; repeat in `--mode compile`.
For macOS sampling run an original binary with `--repeat 100 --profile q51` and, from a
second terminal, `sample <pid> 6 1 -mayDie -file <output>`. Stop that profiling process
after sampling. Do not run builds, validation, or another query alongside timing runs.

Restore the normal workspace example with `cargo build --locked --release -p roc-datafusion
--example tpcds` after using a shared Cargo target directory.
