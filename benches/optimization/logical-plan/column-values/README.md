# ScalarValue / ColumnValue expression amendment

October 5, 2026. Arrow 59.3.0, release builds on Apple M1 Pro. This experiment compares the preceding PR #4 RunArray implementation (`f6f3fcbb8261e4e0128bed762db1c76333fb28af`) with the amended scalar/array implementation (`abb68b16370b000c4938bd5037932e576489e235`). It measures **bound expression evaluation**, including conversion of the final result to an ordinary array. It does not measure SQL planning, storage, DataFusion execution, the later PR #12 Arrow comparison change, or full queries.

A constant now evaluates to `ColumnValue::Scalar(ScalarValue)`. Numeric and Boolean values are stored inline, while types without an inline representation use Arrow `Scalar<ArrayRef>` with one value. `ColumnValue::Array` retains an ordinary Arrow array. The bound function pointer has one signature, `fn(&[ColumnValue]) -> Result<ColumnValue>`; each function dispatches scalar/array combinations internally. Scalar/scalar operations preserve scalar results, and array/scalar operations use Arrow unary kernels with the primitive read once. There are no separately bound left-scalar/right-scalar eval functions or RunArray materialization helpers. Consumers that require columns call `into_array(num_rows)`.

The input is an Int64 column with values `row % 1000`, either without NULLs or with every fourth row NULL. Each case validates exact values and validity against the preceding implementation before timing. Both snapshots are linked into one executable; only their package names are changed so Cargo can disambiguate equal versions. Three independent processes each take nine alternating-order paired samples after 100 warmup evaluations. Each sample repeats 4000 evaluations at 8192 rows or 500 at 65536 rows. The table reports the median of three process medians. Negative elapsed change means lower elapsed time. Source commits, compiler, binary digest and detailed scope are in [metadata.json](metadata.json); process medians are in [summary.json](summary.json).

| Rows | NULLs | Expression | RunArray µs | ColumnValue µs | Elapsed change |
| ---: | ---: | --- | ---: | ---: | ---: |
| 8192 | 0% | `x + 1` | 5.924 | 5.236 | -11.6% |
| 8192 | 0% | `x + (1 + 2)` | 11.392 | 5.190 | -54.4% |
| 8192 | 0% | `x > 250 AND x < 750` | 11.154 | 9.802 | -12.1% |
| 8192 | 25% | `x + 1` | 6.456 | 5.734 | -11.2% |
| 8192 | 25% | `x + (1 + 2)` | 11.134 | 5.787 | -48.0% |
| 8192 | 25% | `x > 250 AND x < 750` | 11.359 | 10.037 | -11.6% |
| 65536 | 0% | `x + 1` | 35.760 | 35.113 | -1.8% |
| 65536 | 0% | `x + (1 + 2)` | 69.687 | 35.131 | -49.6% |
| 65536 | 0% | `x > 250 AND x < 750` | 76.300 | 74.887 | -1.9% |
| 65536 | 25% | `x + 1` | 40.777 | 40.618 | -0.4% |
| 65536 | 25% | `x + (1 + 2)` | 65.245 | 40.609 | -37.8% |
| 65536 | 25% | `x > 250 AND x < 750` | 77.546 | 76.528 | -1.3% |

At 8192 rows, the simple arithmetic and conjunction cases use about 11–12% less time. Their 65536-row differences are small (about 0.4–1.9%) and do not establish a general large-batch improvement. The nested scalar expression eliminates an intermediate broadcast and uses about 38–54% less time. That case is constructed directly as a Roc expression tree: an optimizing SQL planner may already fold `1 + 2`, so its delta is not a claim about such an optimized SQL query.

Checked integer overflow, NULL propagation and eager conjunction argument order remain unchanged. The complete PR #4 passed 98 release tests; the final core stack passed 122; the separately retained local DataFusion workspace passed 138 including its documentation example. The original query and conjunction-only benchmark archives remain historical evidence and are not re-labelled as measurements of this revision.

Raw paired samples: [process 1](process-1.csv), [process 2](process-2.csv), [process 3](process-3.csv).

To reproduce from a repository containing both exact source commits:

```sh
python prepare.py
cargo build --release --locked
cargo run --release --locked > samples-local-1.csv
cargo run --release --locked > samples-local-2.csv
cargo run --release --locked > samples-local-3.csv
```

The benchmark is a standalone package, not a Roc workspace member. It includes no DataFusion integration or dependency. The `.sources` exports are ignored rather than duplicated in this documentation PR. Git must have both recorded commit objects; fetch a missing object before preparation. Re-run outputs are local samples rather than replacements for the recorded evidence.

## Explicit scalar type coverage amendment

PR #4 now follows the explicit representations in DataFusion 55.1.0's `ScalarValue`: Float16, Date/Time/Timestamp, Duration/Interval, Decimal32/64/128/256, string/binary variants, and typed single-row List/ListView/FixedSizeList/Struct/Map arrays. Union, Dictionary and RunEndEncoded values retain their encoding metadata and a recursively represented scalar. Remove the generic `Arrow(Scalar<ArrayRef>)` fallback. Roc retains `Null(DataType)` for compatibility with its existing typed-null constructor; no DataFusion dependency is added to the core. This expands scalar representation and Arrow conversion coverage, without expanding the existing arithmetic binding signatures.

Scalar extraction honors logical slice positions and validity. Array reconstruction preserves Decimal precision/scale, timestamp timezone and nested/encoded fields. Nested scalar variants reject arrays whose length is not one; invalid encoded metadata and run-end size overflow return errors. Ordinary consumers still call `ColumnValue::into_array` to broadcast.

Release validation passed 106 tests at PR #4, 130 at the final core checkpoint, and 146 in the separately retained local DataFusion workspace including its documentation example. Eight new test groups compare reconstructed values with independent Arrow take/cast results across NULLs, sliced arrays, empty outputs and repeated rows. All twelve core PR checkpoints passed all-target compilation; the early global aggregate checkpoint passed its eleven release regression tests. Final core/test trees exactly match the local harness. Exact current source identities appear under `explicit_scalar_types_amendment` in [core-source-verification.json](../core-source-verification.json).

No performance measurements were made for this type-coverage amendment. The expression experiment above measured the earlier `abb68b16370b000c4938bd5037932e576489e235` implementation, which still had the generic Arrow fallback. All recorded timings retain those original source identities.

## Consolidated nested scalar broadcasting

Move the former `constant.rs::repeat` body into `value.rs::repeat_nested`, alongside its single-row validation, and delete the forwarding call and old helper. Zero-row outputs use an empty slice, one-row outputs retain the input Arc, NULL scalars construct a typed NULL array, and other nested values broadcast through Arrow take with repeated zero indices. Production broadcasting is private to scalar value conversion. Test reference arrays use Arrow take directly.

Fresh release validation passed 106 tests at PR #4 and 130 at the final core checkpoint. The separately retained local DataFusion harness mirrors the final core/test trees exactly; its preceding 146-test workspace result remains recorded under the explicit type-coverage amendment. This consolidation adds no public API or algorithm change and has no new performance measurement. Current source identities appear under `nested_broadcast_consolidation` in [core-source-verification.json](../core-source-verification.json).

## Separate scalar and array conjunction updates

Move ColumnValue representation dispatch to the streaming conjunction evaluator. Its scalar arm calls `update_scalar(&ScalarValue)`, and its array arm calls `update_array(&ArrayRef)`. Delete the mixed-input `update(&ColumnValue)` function and its unreachable representation branch. Each private update function keeps its previous value/validity algorithm. Scalar-only expressions retain scalar results; the first array creates the bitmap workspace and seeds it with the preceding scalar result. Argument evaluation order, SQL NULL behavior, length/type errors, immutable inputs and buffer reuse are preserved.

Fresh release validation passed 106 tests at PR #4 and 130 at the final core checkpoint. Existing tests compare scalar/array mixtures and every intermediate bitmap with Arrow Kleene, including NULLs, slices, buffer identity, malformed lengths/types and ordered errors. The local DataFusion harness mirrors the final core/test trees; its preceding 146-test workspace result remains historical. This structural change has no new performance measurement. Current identities appear under `conjunction_update_dispatch` in [core-source-verification.json](../core-source-verification.json).
