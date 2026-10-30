# Role 01 — binary inference and numerical stability

Initial independent audit completed 2026-10-30. Baseline PR220 `38cbe91d5b315584575b132a596890e69eb210aa`, with PR217 comparison. No candidate code was edited. No other role's findings were read before this report. The frozen independent oracle SHA256 is `b87ea41de0a571dd70d59cb31b61c92f2ffaf2f6a0c63a5ba78c6b90241db227`.

## Decision

Retain Wilson and Newcombe as explicitly approximate intervals, after fixing the accepted-confidence numerical boundary. Retain Fisher's exact test for independent binary outcomes. The ordinary formulas are implemented correctly on the tested domain. Restrict automatic binary z-test significance and explicit binary Welch in the safe surface: currently admissible Bernoulli designs have severe false-positive inflation. An exact binary default is preferable to claiming that a support threshold makes an asymptotic test exact.

## Function-by-function matrix

| Function or public route | Initial result | Release recommendation |
|---|---|---|
| `primitives.wilson_ci` | 294 ordinary scalar cases match independent score quadratic; boundary defect at highest valid float level | Retain after stable quantile fix; label approximate |
| `primitives.newcombe_ci` | 9,604 tables match independent method 10; swap and complement invariance pass | Retain after shared quantile fix; no uniform coverage claim |
| `primitives.two_proportion_z` | 9,506 nondegenerate cases match independent pooled-z calculation | Formula retained only as an explicitly asymptotic option under a supported domain; avoid current binary default |
| `ABTest._test_cell` / `Eventstream.ab_test_data` binary auto route | Pooled support allows one unit in an arm and a significant verdict without warning | Restrict or replace binary routing with exact Fisher; P1 inference guard defect |
| `stats.experiments._tests` Fisher / `experiment_data` Fisher | 9,604 SciPy tables match integer combinatorial oracle; eight public fixtures match | Retain for fixed independent Bernoulli units |
| `primitives.welch_t` / `welch_samples`, applied to binary samples | 9,072 ordinary Bernoulli cases match independent unbiased-variance/Satterthwaite formula; 9,072 scale/sign checks pass | Numerical implementation passes; restrict sparse binary use |
| `experiment_data(... tests={'methods': {'conversion': 'welch'}, ...})` | Exposes severely miscalibrated sparse-binary Welch with `test_status='ok'` | Prefer Fisher-only binary testing in safe surface; estimator restriction, not a Welch arithmetic defect |
| `ABTest.alpha` / `_adjust` | Rounds an accepted positive alpha to zero, causing public call failure | Remove numerical rounding or reject a clearly documented unsupported input before calculation; P2 |
| `stats.experiments._binary_intervals` | Ordinary public outputs pass, but sets `status='ok'` on NaN boundary intervals | Require finite endpoints/status consistency; P2 |
| `delta_log_ratio_ci`, `exact_binomial_ci`, MDE, multiple testing, funnels and strata | Not independently certified by this role | Other roles / explicit deferred scope; do not infer coverage from this audit |

## F1 — P1: pooled binary support does not protect the automatic z test

**Classification:** inference guard/product contract defect with a correctly computed asymptotic statistic. Source: `src/retentioneering/tools/ab_test.py:2153` (pooled support), `:2177` (z route), `:2380` (significance verdict). Same public behavior reproduced in PR217 and PR220.

**Small public reproduction:** treatment has 1 success / 1 assigned unit; control has 190 / 1,000. Every unit has an `open` event; successes additionally have a `buy` event. Call `stream.ab_test_data(('arm','treatment','control'), [{'name':'conversion','metric':'has_event','metric_args':{'event':'buy'}}], assignment=assignment)` with defaults. `reproduce.py` builds the complete fixture.

**Actual:** `p_value=0.039364249056830426`, adjusted p unchanged, `verdict='increase'`, `support_warning=''`; Newcombe interval `[0.0161401949, 0.8331107299]`. The independent Fisher value is `0.19080919080919082`. The mere disagreement of two tests is not the defect; exact null enumeration establishes calibration failure.

Under independent Bernoulli outcomes with the same true rate 0.19 and fixed group sizes 1 and 1,000, the current default support rule rejects with probability **0.1697545042**, at nominal 0.05. Across the declared 0.001-spaced grid, maximum rejection is **0.1768210433** at p=0.182. All outcome pairs are enumerated, including those whose p-value is withheld by the default pooled support rule. This is unconditional rejection probability, not rejection conditional on passing support. At p=0.182 Fisher's rejection probability is approximately `6.95e-37` for this extreme design; its conservatism is a real power cost.

**Remedy:** make Fisher the binary confirmatory default, or withhold the z-test verdict for unsupported designs while identifying the method as approximate. A per-arm expected-success/failure threshold of five prevents this case, but is not an exact-size guarantee. On the audited grid, expected-five guarding and a Fisher fallback both still reach **0.0568890803** rejection at n=50/50,p=0.5. Observed-five guarding has essentially the same maximum. Fisher over all binary cases reaches at most **0.0399896003** on this grid; its conditional validity, not that finite maximum, is the reason to prefer it when exact size protection is required.

**Regression:** `reproduce.py --expect-fixed` checks that the one-unit fixture cannot produce an unsupported significant verdict. Preserve the point estimate and honest interval label. Do not use interval/test duality as the test assertion.

## F2 — P1 release restriction: sparse Bernoulli Welch is unsafe despite correct arithmetic

**Classification:** estimator limitation exposed by an unrestricted public method choice. Source: `src/retentioneering/stats/experiments.py:258` accepts Welch for binary metrics; `:288` only requires two units per arm. The raw Welch implementation correctly detects two constant arms, but one constant arm can still supply a misleading variance estimate in a small Bernoulli sample.

**Public reproduction:** control 0/2, treatment 9/100; call `experiment_data` with binary `has_event` mean and `tests={'methods': {'conversion':'welch'}, 'adjustment':'holm'}`. Actual `p_value=0.0023039657126900865`, `test_status='ok'`. The independently calculated Welch value agrees to rounding. Fisher is **1.0**. The optional Newcombe interval is `[-0.5689549887, 0.1622621285]`; disagreement alone is not a defect.

Exact enumeration at n=2/100, common true p=0.085 gives **0.8232391255** unconditional rejection among available Welch results; unavailable outcomes contribute no rejection and have mass **0.0001171341**. Fisher rejects with probability **0.0074468032**. This severe problem arises because an all-zero sample of size two is common and contributes zero estimated variance, while the larger arm estimates a positive mean precisely.

**Remedy:** restrict explicitly binary confirmatory tests to Fisher in the safe surface, or require and clearly document an appropriate approximation regime. A generic minimum of two units does not establish that regime. This finding does not justify disabling Welch for all continuous mean outcomes. The before-fix regression checks this sparse binary public example; it accepts an explicit rejection or unavailable test, or a nonsignificant valid binary result.

## F3 — P2: valid confidence levels return NaN with success status or crash

**Classification:** numerical implementation defect. Source: `src/retentioneering/stats/primitives.py:60` computes `ppf(0.5 + level/2)`; `src/retentioneering/stats/experiments.py:229` unconditionally assigns interval status `ok`; `src/retentioneering/tools/ab_test.py:1733` rounds alpha to 12 decimal places; `:2358` also rounds the corrected alpha.

At `level=float(np.nextafter(1.0, 0.0))` the level is valid and strictly below one, but `0.5+level/2` rounds to one. `wilson_ci(5,10,level)` returns `(nan,nan)`. The frozen independent tail-quantile oracle returns **[0.03281769121412237, 0.9671823087858776]**.

The public `experiment_data` route accepts that confidence level, returns NaN control/treatment/effect endpoints, and labels all three interval statuses **ok**. The public `ab_test_data` route fails with `InvalidParameterError` for **alpha 0.0** after accepting the supplied `ci_level`. It also fails at `ci_level=1-1e-13`, because `ABTest.alpha` rounds a positive alpha to zero. PR217 reproduces the primitive and AB failure.

**Remedy:** use stable survival quantiles such as `norm.isf((1-level)/2)`, retain the unrounded numerical alpha, and apply rounding only to display. Audit the related MDE and t-quantile paths: replacing only Wilson's quantile does not fix alpha rounding or `ppf(1-alpha/2)` elsewhere. If a corrected confidence level cannot be represented, reject or return an explicit numerical status; do not silently claim valid intervals. Add finite-endpoint checks before assigning `ok`.

**Regression:** the minimal runner checks finite Wilson bounds at the highest representable valid level and public calls at both boundary levels. Baseline `--expect-fixed` exits 1 as intended.

## Independent evidence and limitations

The independent oracle was written and hashed before implementation inspection. It uses Python `NormalDist`, elementary arithmetic and exact integer binomial coefficients. Fisher's two-sided ordering is compared using integer probability numerators, avoiding floating tie ambiguity. Only Student-t tail probabilities/quantiles use SciPy; that dependence is shared distribution infrastructure and is disclosed. Existing API fixture signatures were read after freezing; no candidate formulas were copied into the oracle afterward. One pre-inspection self-check was changed from exact zero to a 1e-15 tolerance; both hashes are retained in the amendment log.

The baseline audit has **66,640 recorded comparisons, all passing** on the ordinary input domain. These are comparisons of formulas, invariances and declared availability policies, not a certificate that the methods are calibrated for all inputs. Small-table sizes are all pairs from `{1,2,3,5,10,20,50}`, giving 9,604 outcome tables. Group reversal and binary complementation pass. A scale/sign affine transformation of Bernoulli-valued Welch samples also passes; duplication of counts is correctly understood as changing information, not an invariance requirement. PR217 separately passes 9,604 Newcombe/z comparisons and reproduces F1/F3.

Exact finite enumeration covers **100,100 Wilson parameter points**, **8,748 Newcombe points**, and **12,012 null-rejection points**. There is no Monte Carlo and no seed or simulation confidence interval. Floating arithmetic causes probability-sum deviations around 1e-13; reported results are not symbolic exact decimals. All grids, sample sizes, interval-inclusion tolerance (1e-14), and strict p<0.05 decision rule are in `run_audit.py`. Finite grids do not establish a global worst case.

Wilson's worst observed 95% coverage is **0.794** at n=1,p=0.206. Newcombe's worst is **0.796** at n=1/1,pa=0.005,pb=0.8. Even n=35/15,pa=0.005,pb=1 has coverage **0.8390886104**. These are known approximate-method limitations, not formula defects. Keeping Newcombe is compatible with this evidence only if the interval remains identified as approximate; it cannot be presented as an exact 95% coverage guarantee. `nonduality_examples.json` preserves cases where a Newcombe interval and pooled-z test differ at the threshold. No defect is inferred solely from that mismatch.

Documented policy differs from the preregistered natural limiting convention in two cases: pooled-z returns missing for two all-zero/all-one arms, and Welch returns missing when both arms are constant. Those explicit conservative availability policies are accepted, not scored as defects. Relative effects are undefined at zero control and remain so in tested public cases.

Boundary probes of invalid primitive counts are diagnostic only; these helpers do not enforce all integer/domain constraints and role 07 owns broad API validation. The oracle itself overflows at unrealistically huge n=10^200; that limitation is recorded, and no oracle-based defect claim is made at that size. This audit does not establish causal independence, population correctness, sequential validity, family selection validity, UI fidelity, or package/release reproducibility.

## Reproduce and cross-review

Run from the workspace root:

```sh
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/01_binary/run_audit.py --source .work/ab-safe-bet/baseline220
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/01_binary/reproduce.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/01_binary/baseline220_repro.json --expect-fixed
```

The first command exits 0; the second exits 1 on the frozen baseline by design. To verify a fix without overwriting initial evidence, pass the final checkout as `--source` and a new role-local directory to `run_audit.py --output`; pass a new JSON path to `reproduce.py --output`. The smallest self-contained public fixtures and regression assertions are in `reproduce.py`.

Raw evidence: `comparisons.json`, `comparison_summary.json`, `public_results.json`, `baseline220_repro.json`, `pr217_results.json`, `boundaries.json`, `wilson_coverage.csv`, `newcombe_coverage.csv`, `null_rejection.csv`, `enumeration_summary.json`, `nonduality_examples.json`, `versions.json`, `source_manifest.json`. Earlier harness errors and their corrections are preserved in the command and amendment logs. Material findings still require another reviewer's reproduction and a failing-before/passing-after run; no fix has been implemented by this role.

## Primary references

- NIST, Wilson score interval and binomial confidence intervals: https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm
- Newcombe (1998), original method-10 paper: https://misc.biblio.s3.amazonaws.com/Newcombe98abOnWMWprobCI.pdf
- SciPy official definition of two-sided Fisher probability ordering: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.fisher_exact.html

The report's quantitative results come from the independent local enumeration, not quoted coverage figures from those references.
