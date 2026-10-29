# Independent statistical audit of PR #214

Original PR: https://github.com/retentioneering/retentioneering-tools/pull/214

Audited original head: `aa17374b20407158c687b33abed0fcc3376dbc12`.
The follow-up branch is based on this exact commit. It does not replace, close,
or merge the original PR. Review the follow-up against `codex/experiment-inference-v1`.

## What is tested

The independent generators do not use the tutorial fixture or select samples
based on observed estimates. The production `_draw_sums`, `_summary`,
`_binary_intervals` and `_tests` functions perform inference. Fast public API
checks reconstruct event streams from each of the 15 scenarios and compare
extraction/inference with the same unit matrices. Silent users have **no source
rows**, apart from one deliberately out-of-window history row for the first user.
These API checks are integration checks, not a second full Monte Carlo study.

Each scenario has 2,000 independent repetitions and 1,999 bootstrap resamples per
repetition. A separate sensitivity run uses 1,000 repetitions and 9,999 resamples
for money nulls and the correlated ratio alternative. These use the first 1,000
of the same generated data sets; their bootstrap streams are deterministic.
Boundary scenarios are deterministic in their data: their exact coverage is
known, and binomial intervals there are bookkeeping, not genuine simulation
uncertainty. Simulations are fixed-window independent-unit designs only.

### Known population parameters

- Binary: independent Bernoulli outcomes. Ordinary null p=.1 in both arms;
  alternative .1 versus .13; rare null .001 in both arms; rare alternative
  .001 versus .004; boundary p=0 and p=1. Ordinary balanced groups have 250
  assignments each. Unequal groups have 100 control and 500 treatment units.
- Money: `X=A*exp(Z)`, `A~Bernoulli(.2)`, `Z~Normal(log(100)-2, 2**2)`,
  independent. Thus `E[X]=.2*100=20`, with 80% silent users and a severe
  lognormal tail. The alternative scales treatment by 1.4, so the true
  difference is +8. Balanced n=250, unequal n=100/500, and larger n=1000/1000
  nulls are separate scenarios. All moments exist, but convergence is slow.
- Correlated ratio: `D=1+Poisson(2)`, `L~Gamma(shape=4, scale=20)`, independent;
  `N=D*(L+5*(D-3)+effect)`. Since `E[D]=3`, `Var[D]=2`,
  `E[N]/E[D]=80+10/3+effect`. The numerator and denominator are dependent;
  the true treatment-minus-control ratio difference is 0 or +8.
  This is a ratio of expectations, **not** the mean of individual ratios.
- Sparse ratio: `D~Bernoulli(.01)`, `N=D*Gamma(4,20)`, true ratio 80 in
  both arms, n=100/500. Zero observed/resampled denominators are expected.

Every row reports control/treatment intervals, absolute and relative effects,
status counts, availability, coverage among available intervals, and the joint
probability of producing an interval that covers. An unavailable interval is not
silently discarded or counted as an interval that missed. `coverage_all` is that
joint success probability, not ordinary coverage of a total procedure.

Monte Carlo intervals use SciPy's exact Clopper–Pearson binomial interval, not
the audited Wilson implementation. With 2,000 trials and coverage near .95,
MC standard error is about .0049 (95% half-width about one percentage point).
Intervals are pointwise across scenarios/estimands; isolated deviations need
replication rather than a multiple-comparison fishing expedition.

Fisher/Welch rejections at p<.05 and CI exclusion of zero are distinct outputs.
Under alternatives, test rejection is power, not false-positive rate. Ratio
outcomes do not have a supported hypothesis test: zero available tests must
not be interpreted as zero type-I error. Each test scenario declares a single
metric, so Holm is the identity here. Existing deterministic Holm regression
tests remain in CI; this audit does not certify familywise performance across
multiple selected metrics. Newcombe overrides absolute/arm intervals only;
the `relative_effect` entries in its output still use percentile bootstrap.

## Reproduction

From the repository root, with project Python dependencies installed:

```bash
git show aa17374b20407158c687b33abed0fcc3376dbc12:src/retentioneering/stats/experiments.py > /tmp/pr214-original-experiments.py
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/coverage_validation.py \
  --stats-file /tmp/pr214-original-experiments.py --output /tmp/original.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/coverage_validation.py \
  --output /tmp/fixed.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/coverage_validation.py \
  --trials 1000 --resamples 9999 --workers 2 \
  --scenarios money_null money_unequal_null ratio_effect --output /tmp/sensitivity.json
PYTHONPATH=src python -m pytest tests/tools/experiment_coverage_test.py -q
```

Seed `214202610` is split by fixed scenario index and repetition, then into
separate data/bootstrap streams. Changing worker count or selecting a scenario
does not change its draws. JSON records implementation SHA256, original commit,
checkout commit, library versions and settings. The checkout commit can still
be the original while running local uncommitted corrections; **the implementation
SHA256 identifies the code actually executed**. The frozen original file is
loaded directly and never inferred from master.

Large simulations run only on explicit invocation, not in ordinary CI. The new
44 deterministic/short regression checks take approximately 2–3 seconds locally.
The production change is limited to exact Wilson endpoints at 0/n successes.
No interval method is silently replaced to fit these scenarios.

## Reproducible implementation error

With n=250 and zero successes, the original Wilson lower endpoint was
`8.673617379884035e-19`; with 250 successes, the upper endpoint was
`0.9999999999999998`. At population p=0 or p=1, these data occur with probability
one, so the reported arm interval has **zero exact coverage**. This is floating
point cancellation, not evidence that Wilson theory fails at the boundary.

The correction sets the mathematically exact lower endpoint to 0 when successes
are 0, and upper endpoint to 1 when successes equal n. It retains the original
formula for all other endpoints. Regression checks include multiple n and
confidence levels, independent score-test inversion, and the public API.
Before the correction, 15 of the new checks fail; afterwards all 44 pass.

## Sources

- Newcombe (1998), *Interval estimation for the difference between independent
  proportions: comparison of eleven methods*:
  https://pubmed.ncbi.nlm.nih.gov/9595617/
- Brown, Cai & DasGupta, *Interval Estimation for a Binomial Proportion*:
  https://www.stat.purdue.edu/~dasgupta/publications/tr99-19.pdf

The label 95% is nominal. Wilson/Newcombe have discrete coverage variation;
percentile bootstrap is approximate and can under-cover substantially with
severe tails or limited effective support. A status of `ok` reports that the
algorithm produced an interval, not verified 95% population coverage.

## Exact binary reference

`binary-exact.json` sums the full binomial probability mass over all count
pairs. Wilson intervals come from SciPy, independent of the implementation
under audit; Newcombe endpoints use the published combination. Reproduce with:

```bash
python examples/experiments/binary_exact_validation.py --output /tmp/binary-exact.json
```

At p=.004 and n=250, exact Wilson coverage is 92.0068%, despite the nominal
95% label. In the rare alternative .001 versus .004, exact Newcombe coverage
of the difference is 99.9506%. At p=.1 with n=250/250, exact Newcombe coverage
is 95.1904%. These facts distinguish method discreteness from simulation noise;
a Monte Carlo point estimate is not a more authoritative answer than exact
finite-distribution enumeration. All enumeration probability sums are within
floating point precision of one.
