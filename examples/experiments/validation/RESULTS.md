# Results of the independent PR #214 audit

Read [the protocol, generators and limitations](README.md) before interpreting these tables.

All brackets below are pointwise 95% Monte Carlo confidence intervals. Each primary scenario has 2,000 repetitions. Coverage is conditional on interval availability; availability is shown explicitly. Boundary data are deterministic.

## Absolute treatment-minus-control effect

| Scenario | Interval | Available / 2000 | Coverage [MC 95%] |
|---|---|---:|---|
| binary_null | percentile | 2000 | 94.80% [93.73, 95.73] |
| binary_null | wilson_newcombe | 2000 | 94.25% [93.14, 95.23] |
| binary_effect | percentile | 2000 | 95.20% [94.17, 96.09] |
| binary_effect | wilson_newcombe | 2000 | 95.35% [94.33, 96.23] |
| binary_unequal_null | percentile | 2000 | 95.45% [94.44, 96.32] |
| binary_unequal_null | wilson_newcombe | 2000 | 96.10% [95.16, 96.91] |
| rare_null | percentile | 96 | 100.00% [96.23, 100.00] |
| rare_null | wilson_newcombe | 2000 | 100.00% [99.82, 100.00] |
| rare_effect | percentile | 264 | 100.00% [98.61, 100.00] |
| rare_effect | wilson_newcombe | 2000 | 100.00% [99.82, 100.00] |
| zero_boundary | percentile | 0 | unavailable |
| zero_boundary | wilson_newcombe | 2000 | 100.00% [99.82, 100.00] |
| one_boundary | percentile | 0 | unavailable |
| one_boundary | wilson_newcombe | 2000 | 100.00% [99.82, 100.00] |
| money_null | percentile | 2000 | 91.10% [89.77, 92.31] |
| money_effect | percentile | 2000 | 87.05% [85.50, 88.49] |
| money_unequal_null | percentile | 2000 | 82.80% [81.07, 84.43] |
| money_large_null | percentile | 2000 | 91.80% [90.51, 92.97] |
| ratio_null | percentile | 2000 | 94.60% [93.52, 95.55] |
| ratio_effect | percentile | 2000 | 94.40% [93.30, 95.37] |
| ratio_unequal_null | percentile | 2000 | 93.85% [92.71, 94.86] |
| ratio_sparse_null | percentile | 0 | unavailable |

## False-positive decisions under the null

CI exclusion of zero is not a Fisher/Welch p-value. No formal ratio test is supported.

| Scenario | Procedure | False-positive rate [MC 95%] |
|---|---|---|
| binary_null | percentile CI | 5.20% [4.27, 6.27] |
| binary_null | Fisher | 4.10% [3.27, 5.06] |
| binary_null | wilson_newcombe CI | 5.75% [4.77, 6.86] |
| binary_unequal_null | percentile CI | 4.55% [3.68, 5.56] |
| binary_unequal_null | Fisher | 3.60% [2.83, 4.51] |
| binary_unequal_null | wilson_newcombe CI | 3.90% [3.09, 4.84] |
| rare_null | percentile CI | 0.00% [0.00, 3.77] |
| rare_null | Fisher | 0.00% [0.00, 0.18] |
| rare_null | wilson_newcombe CI | 0.00% [0.00, 0.18] |
| zero_boundary | Fisher | 0.00% [0.00, 0.18] |
| zero_boundary | wilson_newcombe CI | 0.00% [0.00, 0.18] |
| one_boundary | Fisher | 0.00% [0.00, 0.18] |
| one_boundary | wilson_newcombe CI | 0.00% [0.00, 0.18] |
| money_null | percentile CI | 8.90% [7.69, 10.23] |
| money_null | Welch | 2.40% [1.77, 3.17] |
| money_unequal_null | percentile CI | 17.20% [15.57, 18.93] |
| money_unequal_null | Welch | 10.45% [9.14, 11.87] |
| money_large_null | percentile CI | 8.20% [7.03, 9.49] |
| money_large_null | Welch | 3.35% [2.61, 4.24] |
| ratio_null | percentile CI | 5.40% [4.45, 6.48] |
| ratio_unequal_null | percentile CI | 6.15% [5.14, 7.29] |

## More resamples do not fix missing tail information

1,000 repetitions using the first 1,000 primary data seeds, now with 9,999 bootstrap draws. These are paired sensitivity experiments, not another independent primary sample.

| Scenario | Absolute-effect coverage [MC 95%] |
|---|---|
| money_null | 90.70% [88.73, 92.43] |
| money_unequal_null | 81.70% [79.16, 84.05] |
| ratio_effect | 94.90% [93.35, 96.18] |

## Interpretation

- **Implementation error, corrected:** exact Wilson endpoints at p=0 and p=1. Original arm coverage was exactly zero at the chosen n=250 boundary cases; corrected coverage is exactly one. Paired original/fixed reports differ only in the four affected arm-coverage rows.
- **Substantial method limitation:** severe zero-inflated lognormal money under-covers even at n=1000 per arm. With 100/500 groups, percentile CI false positives are 17.2%, and Welch false positives are 10.45%. Increasing resamples does not repair this. No claim is made that all monetary data have this behavior.
- **Discrete method limitation:** exact Wilson coverage at p=.004,n=250 is 92.0068%; exact Newcombe coverage in the rare alternative is 99.9506%. See binary-exact.json. Simulated deviations from those exact rates are Monte Carlo noise.
- **Limited evidence:** balanced correlated-ratio results are compatible with nominal 95%. Unequal ratio coverage is 93.85% [92.71,94.86], a modest deficit in this scenario. A pointwise interval across many scenarios is not a familywise test; do not infer a universal calibration certificate or a programming defect from this alone.
- **Availability matters:** rare-null bootstrap absolute effects are available in only 96/2000 trials. Their 100% conditional coverage is based on that selected 4.8%, not the full procedure. Sparse ratios produce no interval in all 2000 trials; coverage is therefore unassessed, not 100% or an ordinary 0% coverage result.
- **Relative effects:** all estimands and status counts are in the JSON. For money +8, relative-effect coverage is 89.6% [88.18,90.90]; sparse conversion relative intervals are unavailable. Newcombe does not replace relative-effect percentile intervals.

## Validation and change history

1. Start from original PR #214 commit aa17374b20407158c687b33abed0fcc3376dbc12.
2. Run the original implementation, independently reproduce the numerical endpoint failure, and record failing regressions (15 failures among the new 44 checks).
3. Correct only the exact Wilson endpoints; re-run the identical generators/seeds and preserve both reports.
4. Commit evidence and explicit method limitations separately from the code correction. The original PR remains open, with no merge or force-push.

Local validation: **2220 Python tests passed, 1 skipped** (browser suite excluded; no UI code changed). The new 44 checks took about 2.5 seconds; 114 experiment-focused checks passed. Documentation rendering, pre-commit lint/format/hygiene checks and secret scanning passed. Large Monte Carlo runs are opt-in and add no routine CI simulation workload.

## Raw records

- [Original implementation](pr214-original.json)
- [Corrected implementation](pr214-fixed.json)
- [Resample sensitivity](pr214-resample-sensitivity.json)
- [Exact binary probabilities](binary-exact.json)
