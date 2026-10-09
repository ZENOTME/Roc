# Direct conjunction validity: benchmark and ablations

This standalone Arrow 59.3 package measures four-input Boolean bitmap merging. The final `src/fused.rs` is an exact snapshot of the measured independent PR #17 revision recorded in `summary.json`, before the subsequent file-layout/binding simplification; no Roc or DataFusion integration dependency or workspace member is introduced. The general Arrow Kleene sequence represents DataFusion's general bitmap merge only, excluding its executor, expression evaluation, short-circuiting and preselection.

## Implementations

- `fused_before` (`old.rs`): previous optimized one-pass Roc loop, before the scratch revision. It is not the original main implementation that allocated intermediate arrays.
- `arrow_composed` (`reused.rs`): the immediately preceding PR #17 revision. NULL value bits are normalized in reusable scratch; subsequent both-nullable merges use five Arrow bitmap passes.
- `direct_validity` (`direct.rs`): remove scratch and calculate validity in one Arrow binary operation, advancing two additional value iterators in its closure, then update values. This rejected prototype regressed despite fewer passes.
- `separate_validity` (`separate.rs`): directly zip the four inputs to update validity in one pass, then use Arrow to update values in a separate pass. This rejected prototype isolates the extra pass and closure-driven iteration from the final loop.
- `fused_direct` (`fused.rs`): keep the four optional-validity cases. Ordinary values use Arrow binary operations; single-sided validity uses Arrow operations. Both-sided validity uses Arrow slice-aware word iterators and one safe fused loop to update both output bitmaps. No scratch and no NULL value-bit normalization. Both result allocations are reused.
- `df_arrow_kleene`: repeated general Arrow `and_kleene`/`or_kleene` calls, allocating new result bitmaps.

With values `a,b` and validity `av,bv`, the final AND validity is `(av & bv) | (av & !a) | (bv & !b)`; OR substitutes `a,b` for `!a,!b`. A decisive bit must be valid. Compute from original values before replacing them. When only one side has validity, AND validity is `av | !b` or `bv | !a`; OR uses `av | b` or `bv | a`. Both absent means all rows valid. NULL payload bits remain arbitrary. The current Arrow in-place binary API has no four-input, two-output form; the fused nullable loop is the only custom bitmap loop, and still uses Arrow to read sliced inputs. No new unsafe code is added.

## Method

Apple M1 Pro, macOS ARM64, Rust 1.95.0, release builds. Single thread; four deterministic mixed Boolean inputs; nullable cases have 25% NULLs; slice offsets 0/7/14/21. Exact logical output equality is checked for every implementation before timing. Three independent processes record nine samples per case, reversing all six implementation orders between samples. Each timed iteration creates an accumulator, performs four updates and finalizes it. Warm input preparation, planning and expression evaluation are excluded. Samples use 4000 iterations at 8192 rows and 500 at 65536 rows. Medians pool 27 samples. No compilation overlaps timed execution. Units below are nanoseconds per four-input batch merge, not query latency.

## Results

| Rows | NULLs | Op | Previous fused | Scratch | Arrow closure | Separate validity | Final fused | General Arrow |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8192 | none | AND | 409.0 | 393.9 | 406.7 | 402.7 | 400.8 | 543.2 |
| 8192 | none | OR | 372.5 | 361.2 | 371.8 | 349.0 | 362.1 | 534.8 |
| 8192 | 25% | AND | 992.9 | 1368.1 | 1679.9 | 1081.4 | 892.3 | 1589.6 |
| 8192 | 25% | OR | 983.1 | 1353.6 | 1656.2 | 1050.5 | 897.4 | 1592.3 |
| 65536 | none | AND | 1529.3 | 1519.8 | 1539.6 | 1527.1 | 1528.2 | 1567.6 |
| 65536 | none | OR | 1421.3 | 1403.1 | 1430.4 | 1410.5 | 1410.4 | 1574.6 |
| 65536 | 25% | AND | 6373.1 | 5363.1 | 10508.3 | 6270.1 | 5426.6 | 7788.8 |
| 65536 | 25% | OR | 6282.1 | 5276.1 | 10424.6 | 6186.2 | 5368.2 | 7794.4 |

Against the immediately preceding scratch revision, final nullable AND/OR merges are 34.8%/33.7% faster at 8192 rows and 1.2%/1.7% slower at 65536 rows. The small large-batch regression is retained explicitly. Against the earlier fused loop, final nullable merges are 8.7–10.1% faster at 8192 and 14.5–14.9% faster at 65536. Compared with the general Arrow Kleene sequence, nullable merges are 43.6–43.9% faster at 8192 and 30.3–31.1% faster at 65536. No-NULL cases use the same Arrow value operation and show small differences; no systematic new speedup is claimed there.

Removing scratch removes one lazily allocated bitmap and its normalization/copy passes. Comparing `direct_validity` to `separate_validity` measures the complete change in traversal strategy, including iterator advancement/checking and compiler effects; it does not isolate one instruction-level cost. Comparing `separate_validity` to `fused_direct` demonstrates the benefit of this fused implementation under the stated inputs. No SIMD or exact CPU-level causal attribution has been established, and these results do not establish end-to-end query improvements.

`summary.json` includes exact source commits and source SHA-256 digests. `samples-0.csv` through `samples-2.csv` retain all measurements. Earlier scratch-revision samples remain unchanged in the neighboring `conjunction-arrow` directory and describe their recorded source, not the current revision.

Reproduce from the repository root:

```sh
cargo run --release --locked --manifest-path benches/optimization/logical-plan/conjunction-direct/Cargo.toml
```

The current PR places the unchanged bitmap algorithm directly in `conjunction.rs` and removes the forwarding `bind` method. That structural refactoring was not newly benchmarked; the source digests and samples above retain their measured identities.
