# Heavy-tail interval comparison on the PR #216 implementation

Base: `52615dd93476ef8532487792f26d023e944dfe77`, the inspected head of
[PR #216](https://github.com/retentioneering/retentioneering-tools/pull/216).
This is a new paired audit of that implementation, not a relabelling of PR #215.
The research implementations live in `examples/experiments/heavy_tail_validation.py`.
They are not public API options or a calibrated fallback selector.

## Decision

**Keep the existing public methods and make their limits more explicit. Do not
add a purported heavy-tail fix to the inference API from this study.**

There is a real local improvement: for the original 100/500 money null,
symmetric bootstrap-t has 95.40% coverage [94.39, 96.28] and 4.60% false-positive
rate, versus percentile's 82.80% [81.07, 84.43] and 17.20%. The paired coverage
gain is 12.60 percentage points (MCSE 0.75 pp). This is not a universal win:
under the balanced null symmetric bootstrap-t is 99.50% conservative, and its
relative-effect coverage is only 87.65%. BCa and equal-tailed bootstrap-t make
the original unequal-null problem worse (74.55% and 79.05% coverage).

The decisive counterexample keeps the same mean estimand and revenue family:
control n=500, treatment n=100, true treatment effect +8. The primary 2,000-trial
run gives symmetric bootstrap-t only 83.30% coverage [81.59, 84.91]. An independent
1,000-trial replication gives 83.90% coverage
[81.47, 86.13]. Basic, BCa, equal-tailed bootstrap-t, m-out-of-n and Welch also
under-cover substantially there. Good calibration at a zero-effect,
identical-distribution null therefore cannot qualify a confidence interval for
unknown nonzero effects. This failure is much larger than Monte Carlo uncertainty.

These findings do not make the retained percentile/Welch procedures calibrated.
They justify retaining explicitly labelled approximations, documenting when
inference is not supported by enough tail information, and avoiding a misleading
new safety label. A future method should improve a prespecified range of tails,
allocations and alternatives, and state the assumptions supporting its interval.
No data-dependent selector is inferred from these simulations.

Full tables, including all methods and availability, are in
[HEAVY_TAIL_RESULTS.md](HEAVY_TAIL_RESULTS.md). Raw trial records are the
`heavy-tail-*.json.gz` files in this directory. No production source or public
API is changed by this follow-up; all new interval implementations are research
candidates in the explicitly invoked audit CLI.

## Statistical target and protocol

For revenue the target stays the arithmetic population mean difference

$$\Delta=E[X_T]-E[X_C],\qquad \widehat\Delta=\bar X_T-\bar X_C.$$

All assigned units, including the structural zeros, remain in each arm. No
log transformation, trimming, winsorization, outlier deletion or conditioning
on a payment is used. Ratio scenarios still target ratios of expectations,
not averages of individual ratios. Relative effects remain $\mu_T/\mu_C-1$.

The original 15 generators, scenario indices, analytic truths and data seeds
are imported without modification from `coverage_validation.py`. Main runs
use 2,000 repetitions and 1,999 resamples, seed `214202610`. New scenarios
are appended, so they cannot move an original seed: money 500/100 null,
100/500 effect +8 and 500/100 effect +8. These check allocation orientation
and nonzero effects; they are not independent replications of original scenarios.

Each generated dataset is shared across methods. The same within-arm multinomial
weights serve percentile, basic, BCa and both studentized variants. The baseline
uses the unchanged production `_summary`, `_welch` and binary interval routines.
The augmented draw routine additionally computes second moments; regression
checks compare its sums against production and its SEs against literal repeated
observations. Numerator/denominator pairs always share a weight.

Secondary runs use the first 1,000 data seeds with 9,999 resamples for the
balanced and unequal money nulls. These are paired sensitivity runs, not new
independent Monte Carlo evidence. The m-out-of-n tuning sensitivity changes the
power from .7 to .85 on the first 1,000 primary datasets, holding B=1,999.
There is no data-dependent method selection or tuning to maximize coverage.

An independent seed `214202611` repeats the unequal null, unequal +8 effect and
reversed-allocation +8 effect, 1,000 trials each with B=1,999. The reversed-effect
replication follows the pilot failure and explicitly checks it on new datasets;
it is not combined with the discovery trials to imply a preregistered test.

Every method reports all four estimands, availability, unavailable reasons,
conditional coverage and joint availability-and-coverage. Rejections based on
CI exclusion of zero are separate from production Fisher/Welch tests. Under
alternatives rejection is power, not false-positive rate. Unsupported ratio
tests and Welch relative intervals are unavailable, not successful non-rejections.
All error rates are fixed-window, single-metric and pointwise; Holm is identity.

For a count $K$ in $R$ trials, the Monte Carlo standard error is

$$\operatorname{MCSE}(\hat p)=\sqrt{\hat p(1-\hat p)/R}.$$

Tables use exact Clopper-Pearson 95% Monte Carlo intervals. For available-only
rates, the denominator is the actual number of available intervals. At 95%
coverage and R=2,000, MCSE is approximately 0.49 percentage points. Paired
coverage improvements use $D_r=I_r^{\rm candidate}-I_r^{\rm percentile}$ and
$s_D/\sqrt R$, preserving dependence instead of treating methods as independent.
The JSON includes trial decision bit strings for independent paired analysis.
MC intervals are pointwise across the many methods/scenarios; small isolated
departures are not evidence of a universal failure or superiority.

## Computational cost and numerical checks

Studentization uses analytic within-resample moments, so it has the same
O(B(n_C+n_T)) order as ordinary resampling, with five aggregated columns instead
of two. It does not require B times another inner bootstrap. Basic and BCa reuse
draws; the stratified analytic jackknife here is O(n_C+n_T). The m-out-of-n
prototype performs a second resampling pass. Welch avoids resampling entirely.

An illustrative local component benchmark (20 measured repetitions after
warm-up, B=1,999, one money metric, no event extraction/UI) gives these medians:

| Method | n=250/250 | n=100/500 | n=1000/1000 |
|---|---:|---:|---:|
| Production percentile | 46.80 ms | 55.89 ms | 181.10 ms |
| Basic prototype | 48.08 ms | 57.06 ms | 182.82 ms |
| BCa prototype | 49.14 ms | 58.36 ms | 184.74 ms |
| Equal-tailed bootstrap-t | 48.07 ms | 57.14 ms | 182.82 ms |
| Symmetric bootstrap-t | 48.07 ms | 57.09 ms | 182.84 ms |
| m-out-of-n prototype | 75.89 ms | 91.70 ms | 281.74 ms |
| Welch | 0.43 ms | 0.44 ms | 0.53 ms |

These are shared-executor wall-clock component estimates, not a portable
performance claim. Candidate costs include the shared augmented draw pass plus
only the candidate's postprocessing. The rejection of a new public method is
statistical, not due to a computational bottleneck. Reproduce with
`heavy_tail_benchmark.py`; the full measurements are in `heavy-tail-benchmark.json`.

Eight fast regression test methods cover literal-resample moment references,
studentized quantile inversion, SciPy BCa with unequal groups, SciPy Welch,
production-baseline agreement on all 15 original scenarios, unit rescaling,
invalid studentizers, and the m=n basic-bootstrap limit. Large Monte Carlo
coverage thresholds are not flaky CI assertions. Ruff lint/format also pass.
This workspace validation is focused on the research code; it is not a claim
that the full library/browser/notebook suite was rerun locally.

## Methods and rationale

Let $\theta$ denote one of the four estimands, $q_p$ an empirical bootstrap
quantile, and $s$ its estimated standard error.

| Method | Construction | Relevant limitation |
|---|---|---|
| Percentile | $[q_{.025}(\hat\theta^*),q_{.975}(\hat\theta^*)]$ | Empirical tail support can be badly incomplete |
| Basic | $[2\hat\theta-q_{.975}(\hat\theta^*),2\hat\theta-q_{.025}(\hat\theta^*)]$ | Reflects the bootstrap error; no studentization |
| BCa | Bias-corrected, accelerated quantile probabilities | Higher-order asymptotics do not guarantee finite-sample improvement |
| Bootstrap-t, equal-tailed | $[\hat\theta-q_{.975}(T^*)s,\hat\theta-q_{.025}(T^*)s]$ | Estimated SE can be unstable, zero or strongly coupled to the estimate |
| Bootstrap-t, symmetric | $\hat\theta\pm q_{.95}(|T^*|)s$ | Can become very wide; total coverage can conceal unequal tail errors |
| m-out-of-n basic | Reflect rescaled arm-wise errors with $m_j<n_j$ | Requires a tuning sequence and an appropriate convergence rate |
| Welch | $\widehat\Delta\pm t_{\nu,.975}\sqrt{s_T^2/n_T+s_C^2/n_C}$ | Asymptotic approximation for these nonnormal populations |

Here $T^*=(\hat\theta^*-\hat\theta)/s^*$. For a difference of means, both the
original and resampled SE use separate sample variances divided by their own
arm sizes. There is no pooled equal-variance assumption and no nested bootstrap.
For a ratio $\hat r=\sum N_i/\sum D_i$, the delta-method variance is

$$\widehat{\operatorname{Var}}(\hat r)=
\frac{\sum_i(N_i-\hat rD_i)^2}{n(n-1)\bar D^2}.$$

Independent-arm variances add for the absolute difference. The relative effect
uses gradient $(-\hat r_T/\hat r_C^2,1/\hat r_C)$ and the same covariance-free
independent-arm propagation. This is an approximation, not a Fieller interval.

BCa uses the midpoint convention for bootstrap ties and the stratified
delete-one jackknife acceleration. Each arm's jackknife influence is divided
by its own n before pooling powers; treating unequal groups as one unweighted
jackknife sample would be incorrect. Tests compare all four mean estimands
with SciPy's independent implementation using the identical bootstrap distribution.

For m-out-of-n, set $r=\min(n_C,n_T)^{\gamma-1}$,
$m_j=\max(2,\lfloor r n_j\rfloor)$ and
$e_j^*=\sqrt{m_j/n_j}(\hat\mu_j^{*(m_j)}-\hat\mu_j)$.
Invert the quantiles of $e_T^*-e_C^*$ around the full-sample effect. This is a
finite-variance, root-n basic method, not a method for infinite-variance tails.
The common fraction preserves the allocation ratio asymptotically. Relative
errors use the delta gradient at the original point. Changing gamma to .85
is a sensitivity check, not an automatic tuning rule.

Studentized intervals are unavailable if any requested bootstrap studentizer
is zero/nonfinite; invalid draws are not silently dropped. Other bootstrap
methods inherit production denominator/degeneracy guards. BCa also rejects
undefined or singular adjusted probabilities. No infinite intervals, clipping
of endpoints or post-hoc fallback are counted as successes. The JSON records
width median, 90th and 99th percentiles, and both directions of noncoverage.

## Why this generator is difficult

The unchanged money generator is $X=A\exp Z$, with $A\sim\operatorname{Bernoulli}(.2)$
and $Z\sim N(\log100-2,4)$ independent. Its mean is 20, SD is about 329.84,
skewness about 906.89 and standardized fourth moment about 44,757,640.
All moments exist; their magnitude still makes asymptotic corrections unreliable
at ordinary experiment sizes.

The top 0.1% of all assignments (amounts above approximately 2,337.48) account
for 28.24% of expected revenue. A sample of 100 misses that entire tail with
probability $.999^{100}=90.48\%$; even n=1,000 misses it 36.77% of the time.
These numbers follow analytically from the generator, not from fitted sample
tails. Increasing B only reduces conditional quantile simulation error.

## Other defensible directions, and their additional contracts

- **Catoni and median-of-means:** these can still estimate the original arithmetic
  mean. They must not be dismissed as changing the estimand merely because the
  estimator is robust. However, turning their deviation bounds into a numeric
  confidence radius requires a justified scale/moment bound or another suitable
  assumption. Replacing an unknown population bound with the same unstable
  sample variance is not a finite-sample guarantee. Oracle population variance
  from this generator would give these methods information unavailable to the
  other candidates, so such a comparison is deliberately not presented.
- **Known support or tail bounds:** an actual business-level maximum, or a
  defensible upper bound on variance/kurtosis, permits conservative concentration
  intervals for the same mean. A sample maximum is not a known support bound.
  Adaptive truncation with an explicit bias bound can target the original mean;
  ordinary fixed winsorization without that correction targets a different mean.
- **Parametric zero-inflated lognormal inference:** it can target
  $p\exp(\mu+\sigma^2/2)$ and hence the same mean, but only with a new modelling
  contract. It is particularly attractive on a generator chosen from that exact
  family, which is not evidence of robustness to real revenue mixtures.
- **Permutation inference:** identical-distribution nulls permit exact permutation
  tests even with unequal n. Equality of means is a weaker null; heteroskedastic
  populations need studentization and then generally only asymptotic validity.
  A test that controls error at the identical-distribution null alone does not
  establish coverage of the +8 scale alternatives in this audit.
- **Empirical likelihood and double bootstrap:** these are additional research
  candidates, not tested implementations here. Smooth-mean asymptotic refinements
  still require tail support/regularity; no finite-sample heavy-tail guarantee is
  inferred from their names.

Uniformly valid bounded confidence intervals over unrestricted mean/variance
classes cannot be recovered simply by choosing a better bootstrap. This is
different from saying inference for every particular heavy-tailed distribution
is impossible. Explicit restrictions can make useful inference possible.

## Sources

1. Hall (1988), [Theoretical Comparison of Bootstrap Confidence Intervals](https://doi.org/10.1214/aos/1176350933).
2. Hall (1988), [On Symmetric Bootstrap Confidence Intervals](https://academic.oup.com/jrsssb/article/50/1/35/7027851).
3. Efron and Narasimhan (2020), [The automatic construction of bootstrap confidence intervals](https://pmc.ncbi.nlm.nih.gov/articles/PMC7958418/).
4. Bickel, Goetze and van Zwet (1997), [Resampling fewer than n observations](https://www3.stat.sinica.edu.tw/statistica/oldpdf/a7n11.pdf).
5. Catoni (2012), [Challenging the empirical mean and empirical variance](https://www.numdam.org/articles/10.1214/11-AIHP454/).
6. Devroye et al. (2016), [Sub-Gaussian mean estimators](https://arxiv.org/abs/1509.05845).
7. Romano and Wolf (2000), [Finite sample nonparametric inference and large sample efficiency](https://www.econ.uzh.ch/dam/jcr%3Affffffff-935a-b0d6-0000-00003463ae9d/annalsRomano.pdf).
8. Chung and Romano (2013), [Exact and asymptotically robust permutation tests](https://arxiv.org/abs/1304.5939).
9. [SciPy bootstrap reference](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html), used as an independent BCa numerical reference.

## Reproduction

From a checkout containing this follow-up, with NumPy, SciPy and pandas installed:

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_validation.py \
  --trials 2000 --resamples 1999 --workers 7 --output /tmp/heavy-tail-primary.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_validation.py \
  --trials 1000 --resamples 9999 --workers 2 --scenarios money_null money_unequal_null \
  --output /tmp/heavy-tail-resample-sensitivity.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_validation.py \
  --trials 1000 --resamples 1999 --m-power .85 --workers 2 \
  --scenarios money_null money_unequal_null --output /tmp/heavy-tail-m-sensitivity.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_validation.py \
  --trials 1000 --resamples 1999 --seed 214202611 --workers 2 \
  --scenarios money_unequal_null money_unequal_effect --output /tmp/heavy-tail-independent.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_validation.py \
  --trials 1000 --resamples 1999 --seed 214202611 --workers 1 \
  --scenarios money_reversed_effect --output /tmp/heavy-tail-reversed-independent.json
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python tests/tools/heavy_tail_validation_test.py -v
```

Worker count and scenario selection do not change data/resample seeds. Raw JSON
records runtime versions, settings and SHA256 hashes of the actual generator,
production inference and comparison harness. Gzip records are lossless; load
with `json.load(gzip.open(path, 'rt'))`. Large simulations remain opt-in and
do not run in ordinary CI.

Provenance detail: the fetched production and original generator files used in
the recorded runs each had one extra terminal LF relative to the pinned Git
blob. Their executable content is unchanged; the raw SHA256 fields intentionally
identify the bytes actually executed. To reproduce those exact file hashes,
append one newline to each of the two pinned files. Normal checkouts produce
the same numerical results without that whitespace-only difference. The
comparison harness hash corresponds to the shipped harness exactly.

To regenerate the tables from the compressed records:

```bash
python examples/experiments/heavy_tail_report.py \
  examples/experiments/validation/heavy-tail-primary.json.gz \
  examples/experiments/validation/heavy-tail-resample-sensitivity.json.gz \
  examples/experiments/validation/heavy-tail-m-sensitivity.json.gz \
  examples/experiments/validation/heavy-tail-independent.json.gz \
  examples/experiments/validation/heavy-tail-reversed-independent.json.gz \
  --output /tmp/HEAVY_TAIL_RESULTS.md
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python examples/experiments/heavy_tail_benchmark.py \
  --output /tmp/heavy-tail-benchmark.json
```
