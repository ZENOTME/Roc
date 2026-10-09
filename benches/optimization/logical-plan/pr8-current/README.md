# Current PR #8 aggregate measurements

This measures main `1bfd41bdcc20f78a58b86957302369825a254076` against PR #8 `6504d61bca4ffc48ca18d90365c068a67a708b45`. Unlike earlier cumulative reports, these binaries include the current single optional argument API and remove COVAR_POP from PR #8. Main still supports COVAR_POP, but no workload calls it.

Each run processes 1,048,576 resident rows and 256 Int64 groups, with 1/7 NULL values. The timer includes AggregateOperator sink construction, grouping, expression evaluation, state updates, partial-state combine, final merge and output. It excludes input generation, disk/Parquet decoding, DataFusion planning and pipeline scheduling. These are aggregate-execution measurements, not full-query or DataFusion comparisons.

Six separately compiled binaries compare main, PR #8, and PR #8 with one change disabled at a time: eager scalar contexts, public group-ID validation, unconditional Arrow cast, or argument-vector allocation. Five process replicas randomize binary order; each has 24 workload configurations and eight timed samples. Case order reverses in odd replicas. All 5,760 timed samples are preserved; each workload first validates all output groups against an independent row-wise reference. All builds use identical dependency lockfiles, Arrow 59.3.0, Cargo release defaults and no LTO override, on Apple M1 Pro.

Reported times are geometric means of the five process medians. Percentages compare those paired process medians. Intervals use a paired bootstrap over the five process log ratios, 10,000 draws, 95% percentile intervals. They describe this machine and workload set; five process replicas do not establish universal performance guarantees. Hash-map seeds remain randomized as in production.

## Main versus PR #8: 2048 rows per batch, 3 columns

| Workload | Main (ms) | PR #8 (ms) | Elapsed-time reduction | 95% interval |
|---|---:|---:|---:|---:|
| count_star | 3.628 | 3.311 | 8.7% | 3.5% to 12.1% |
| filtered_sum | 14.071 | 13.616 | 3.2% | 2.2% to 4.1% |
| mixed | 8.877 | 7.762 | 12.6% | 10.7% to 13.6% |
| sum | 5.160 | 4.807 | 6.8% | 4.1% to 8.8% |

## Individual controls

The results below compare current PR #8 with the otherwise identical source containing one restored operation. They are not additive: compiler output, other operations and workload shape can interact.

| Workload, 2048 rows / 3 columns | Removing eager contexts | Removing repeated ID checks | Removing argument vectors |
|---|---:|---:|---:|
| count_star | -1.6% (-6.1% to 1.6%) | 7.9% (2.2% to 12.0%) | -3.0% (-8.4% to 1.1%) |
| filtered_sum | 0.4% (-0.5% to 1.4%) | 2.3% (0.7% to 3.4%) | -0.3% (-1.8% to 0.8%) |
| mixed | 0.1% (-2.6% to 3.1%) | 10.8% (8.4% to 12.6%) | -0.5% (-2.8% to 1.3%) |
| sum | -2.3% (-5.1% to 0.3%) | 4.8% (1.2% to 8.1%) | -2.6% (-5.7% to 0.4%) |

At conventional batch size, removing eager contexts and argument vectors has no stable benefit in these cases: all those intervals include zero. Removing repeated group-ID checks has a measurable effect, including about 10.8% for mixed SUM/COUNT/AVG.

At 128 rows per batch and 32 columns, eliminating eager contexts reduces COUNT(*) time about 27.9% (26.8% to 28.7%), SUM about 12.7% (11.3% to 14.4%), and mixed aggregation about 16.6% (14.7% to 18.8%). Small batches create contexts more frequently; wide batches require more ArrayRef clones. Wide unused columns share immutable buffers in this synthetic workload, and normal scan projection may already prune them. This scenario is not a universal full-query claim.

The cast control is inconclusive as an explanation of cast cost. Restoring unconditional Arrow cast improves some 2048-row configurations; it also substantially changes COUNT(*) timing even though COUNT(*) never calls cast. Thus the observed difference cannot be attributed solely to the cast function or the avoided wrapper allocation. Compiled code layout/code generation and randomized hashing are possible confounders, not established causes. This experiment does not prove that same-type cast borrowing is a performance improvement, and the unexpected control result remains unresolved. Raw numbers for that control are retained in summary.json and summary.csv.

No production code was changed based on these measurements. The optional-argument API is also a structural simplification; removing COVAR_POP itself is not a measured speedup.

## Reproduce

Copy this directory to a separate output location. The source archives contain the actual standalone source files used by each binary; they include all Roc production source, benchmark source, manifests and dependency lockfiles. metadata.json records source and executable hashes. Original build logs document fresh Roc compilation for every binary. build.py forces source freshness because a shared Cargo target cache can otherwise reuse artifacts across copied workspaces with matching relative paths.

Extract each variant's source.tar.gz inside its variant directory, then run:

```sh
python3 build.py
python3 run.py
python3 summarize.py
```

Build main with the baseline feature to use its Vec-based API; build.py does this automatically. Controls retain the current optional-argument API. No integration crate is required. A source reconstruction setup.py is also retained, but the archives are the portable reproduction source and avoid its original local-checkout path.
