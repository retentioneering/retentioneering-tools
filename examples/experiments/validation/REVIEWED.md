# PR #216: review corrections and validation

This candidate consolidates the assignment-aware API from PR #214 and the Wilson
boundary correction plus statistical audit from PR #215. Its correction baseline
is `047039a2be9605b791ecfc6df47bec9b2cb51b7f`. It targets `master`; the earlier
branches remain available for comparison.

The PR #215 Wilson fix was reviewed against its formula, public API tests and
independent score-inversion checks. Pinning the exact zero/all-success endpoints
is appropriate. Its generators, analytic truths, distinction between availability
and coverage, and separate reporting of test rejections were retained. Its
2,000-trial reports remain frozen evidence for their recorded implementation.

## Corrections from the second review

| Review finding | Correction | Regression evidence |
|---|---|---|
| R1: constant decimals bypass inference guards | Detect structural constancy in original contribution pairs before resampling/variance arithmetic | Decimal means at 10/30/100 units, one constant arm, constant ratios and tiny varying values |
| R2: categorical labels reject a silent assigned arm | Compare realized scalar labels independently of category metadata, preserving numeric/string distinctions | Ordered/unordered categorical rosters, unused categories, silent arm and genuine conflicts |
| R3: all-null CIs crash notebook plots | Normalize nullable numeric columns on a plotting copy; retain points and unavailable statuses | PNG/SVG export for finite, mixed, point-only, all-zero and undefined-point results; input immutability |
| R4: percentage ratios mislabel differences | Resolve an explicit `effect_unit`; percentage-valued rates use pp while relative effects use percent | Public ratio payload/widget parity and React percentage-unit rendering |
| R5: ordinary Funnel materializes every path | Shared step CTEs with aggregate SQL counts for point-only calls and per-unit vectors for Experiment | Oracle/parity, absent/repeated steps, diff groups and population-independent output-shape checks |
| R6: dataset rebuild route disagrees with publication | Correct the command/licence metadata and record serializer versions | Deterministic sample rebuild, receipt mismatch, nonempty-output and source-immutability checks |
| R7: default Lumeo loader directory is wrong | Resolve the included fixture relative to its helper | Equivalent explicit/default loading from different working directories |
| R8: promised baseline history follows assignment | Move only the synthetic baseline timestamp before assignment | History precedes assignment and contributes zero inside the outcome window |
| R9: Welch overflows under common rescaling | Stable common-origin/scale moments with explicit numerical-failure status | Tiny/large scales, representable common offsets and independent SciPy comparisons |

No interval procedure is substituted to obtain preferred simulation results.
The bounded ratio-constancy check permits only a few floating-point rounding
units and has no blanket absolute tolerance around zero. Display units do not
change a ratio's statistical type. A nonrepresentable Welch calculation returns
an unavailable status rather than a misleading successful p-value.

The ordinary Funnel measurement on 100,000 paths and five steps now returns one
row of five counts (172 bytes), instead of a per-path matrix. This is a client
materialization result, not a claim about production runtime on every workload.

All five frozen Lumeo compressed table hashes were reproduced exactly from the
recorded parent sample inputs. No CSV/GZ bytes changed. The manifest adds a
verified rebuild environment, and the builder records the current environment
when run again. Tests also use an independent tiny parent fixture.

## Validation of the revised implementation

- Complete local Python suite excluding browser tests: **2,258 passed, 1 skipped**.
  One existing joblib physical-core discovery warning was reported.
- Revised statistics and public label tests passed with warnings treated as
  errors. An independent review ran 800 Welch comparisons across unequal arms,
  extreme scales and ordering changes; maximum absolute p-value difference
  from the ordinary-scale SciPy reference was approximately `1.44e-15`.
- **19 JavaScript checks passed**, including percentage-point rendering.
- Native Chromium export/tab/resize regression passed.
- Both source tutorials executed: **13 and 17 code cells, zero cell errors**.
- Repository lint/format/hygiene/secret checks and documentation generation passed.
- The sdist and wheel build passed. CI additionally uploads executed tutorial
  outputs and failure diagnostics, so reviewers can inspect the tested commit.
- A fresh wheel-only environment passed **77 focused regression checks** with
  warnings treated as errors and both notebooks (13/17 code cells, no errors),
  without the source checkout on `PYTHONPATH`. Dependency compatibility passed.

The new `pr216-reviewed-smoke.json` uses the reviewed implementation hash and
100 repetitions of each of the same 15 scenarios, with 999 resamples. A paired
run against the frozen PR #215 implementation gave identical coverage,
availability, status and hypothesis-rejection accounting in these 1,500 trials.
The standalone paired comparison record is `pr216-paired-comparison.json`.
This small run is a regression smoke, not a replacement for the larger frozen
audit and not a new nominal-coverage certificate. Structural-constancy corrections
can change availability in other data; the dedicated deterministic tests cover
those cases directly.

Reproduce the current smoke with:

```bash
PYTHONPATH=src OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  python examples/experiments/coverage_validation.py \
  --trials 100 --resamples 999 --workers 2 --output /tmp/reviewed-smoke.json
```

`implementation_sha256` identifies the actual statistics source executed, even
when the checkout commit is the parent and local corrections are present.

## Remaining method limitations

The numerical and integration corrections do not cure the documented severe-tail
undercoverage. In the frozen unequal-arm monetary null, nominal 95% percentile
intervals covered in 82.8% of 2,000 trials and Welch rejected in 10.45%. Increasing
resamples did not remove the problem. The widget and both tutorials now state
that an available result and nominal confidence level do not establish calibrated
coverage for sparse or heavy-tailed outcomes.

The supported design remains fixed-window, two independent arms and pointwise
intervals. Sequential, paired, cluster-randomized or censored designs require
separate inference contracts. A product guardrail label is not a noninferiority
test. Future duration outcomes, baseline adjustment, planning utilities and
generic descriptive funnel uncertainty remain separate extensions.
