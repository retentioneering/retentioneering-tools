# Fresh sensitivity simulations after the initial audit

These are new simulations authored after inspecting the implementation; they are an explicitly disclosed extension, not preregistered independent discoveries. The frozen oracle is unchanged. No historical PR218 runs or other reviewers' fixtures were reused.

All rows below use PR220 `38cbe91d5b315584575b132a596890e69eb210aa`, Python/runtime versions in `baseline220_provenance.json`, 1,000 independent experiments per condition, null effect zero and 95% percentile intervals. The two bootstrap-count settings use the **same experimental datasets** and bootstrap seed, so they are paired sensitivity checks rather than 2,000 independent experiments. Each reported Wilson interval describes binomial uncertainty across 1,000 experiments. All 1,000 intervals are available in every row.

Data seed: `31416329` (`SEED + 402`). Resampling seed: `31435927 + trial`. Data are generated in condition order as saved in `extended_review.py`. This extension calls the exported resampling engine with public scorecard bridge checks on the first three datasets per setting and one additional disagreement case. Raw outcomes and counts: `baseline220_extension_raw.json`, `baseline220_extension_raw.csv`, `baseline220_extension_summary.json`.

| Null population and sizes | B | Covered / 1000 | Coverage, 95% MC interval | Rejected / 1000 | Type I, 95% MC interval |
|---|---:|---:|---:|---:|---:|
| Regular ratio, n=80/120 | 399 | 953 | 95.3% [93.81%,96.45%] | 45 | 4.5% [3.38%,5.97%] |
| Regular ratio, n=80/120 | 1999 | 952 | 95.2% [93.69%,96.36%] | 45 | 4.5% [3.38%,5.97%] |
| Skew ratio, n=15/150 | 399 | 734 | 73.4% [70.58%,76.05%] | 201 | 20.1% [17.73%,22.70%] |
| Skew ratio, n=15/150 | 1999 | 732 | 73.2% [70.37%,75.85%] | 201 | 20.1% [17.73%,22.70%] |
| Skew mean, n=50/500 | 399 | 827 | 82.7% [80.23%,84.92%] | 145 | 14.5% [12.45%,16.82%] |

Regular ratio: denominator `1 + Gamma(shape=2, scale=2)`; numerator = denominator times an independent `Normal(2,.4)` unit value, so the population ratio is 2 in each arm. Skew ratio: denominator `1 + Poisson(3)`; numerator = denominator times an independent `Lognormal(0,2)` unit value, so both population ratios are `exp(2)`. Skew mean: `Lognormal(0,2)` in each arm. These distributions have finite moments and strictly positive denominators; invalid-denominator availability rules do not explain the failures.

The regular ratio results support this explicit method in the tested regular setting. Increasing B from 399 to 1999 barely changes skew-ratio coverage and leaves the rejection count unchanged; poor experimental tail representation, rather than too few bootstrap draws, is the dominant limitation here. Larger arm sizes of 50/500 improve the skew-mean performance but do not restore nominal calibration. A generic “at least 30 supporting paths” threshold is therefore not sufficient evidence for reliable arbitrary continuous-outcome inference.

The saved public disagreement case is a useful disclosure check: `baseline220_disagreement.json`, trial 8 at B=1999, reports delta 3.438451, percentile CI `[0.042988,7.115793]`, p=.054974 and verdict `inconclusive`. Its reading expressly states that the interval excludes zero while p exceeds alpha. Existing implementation disclosure works in this fixture; a mixed-method result alone is not an implementation defect.

These experiments support a narrow default release that does not automatically infer or declare winners for arbitrary heavy-tailed ratios. They do **not** show that ratios or bootstrap inference must always be disabled: the regular-ratio positive control calibrated well, and explicit advanced use with justified assumptions remains reasonable. No universal cutoff or finite-sample coverage guarantee is inferred from this finite set of simulations.
