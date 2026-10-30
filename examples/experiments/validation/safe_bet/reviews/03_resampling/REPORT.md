# Role 03 — Resampling and econometrics review

**Decision:** retain the verified unit-contribution arithmetic and descriptive means/ratios. PR220 is materially safer than PR217 on joint missingness, numeric standard errors and undefined bootstrap draws. Preserve explicit inference assumptions; do not interpret a percentile interval and the normal-bootstrap p-value as the same decision procedure. Restrict paired-change claims to genuinely common eligible units, and reject or handle arithmetic overflow explicitly. The whole-unit bootstrap is correctly implemented for the cases checked; finite-sample heavy-tail failures are estimator limitations, not proof of a faulty resampling kernel.

Reviewed on 2026-10-30: PR220 `38cbe91d5b315584575b132a596890e69eb210aa` and PR217 `7e61d997bcbd9bae9f1571ba3521a85fcb59f751`. All outputs are local to this role. No source edits, other reviewers' reports, repository tests, historical PR218 results or bundled teaching datasets were used as evidence. Repository source was inspected only after the preregistration/oracle freeze. This is an oracle-first audit, not a formal blinded human study.

## Independent method and provenance

The frozen oracle implements arithmetic means, ratio-of-sums, mean-of-ratios, complete vectors of paired contributions, literal index resampling and literal expansion of Poisson weights. Its point estimators use scaling and `math.fsum`. It deliberately distinguishes an undefined denominator from an observed zero contribution. `oracle.interval` reports finite-draw diagnostics after excluding invalid draws; it is a diagnostic distribution summary, **not** approval to report an interval when draws are undefined. Actual API availability is assessed separately.

- Preregistration SHA256: `fd469de1ef7b42db64d2f732fc9e1a27dc524b8756d4f4fc9ca3c72f4f987b73`.
- Frozen oracle SHA256: `9c8d514b45e2ced9b5276c3cda4465c9b1e866a01f6901dd5d200fdefd0c9ac9`.
- Freeze timestamp: `2026-10-30T19:22:24.592036+00:00`; see `preinspection_sha256.json`.
- Python, NumPy, pandas, SciPy, DuckDB versions, imported source paths and file hashes: `baseline220_provenance.json`, `pr217_provenance.json`.
- Runnable public adapters: `run_review.py`; further simulations: `extended_review.py`; source selection uses `PYTHONPATH` and the output label prevents overwriting baseline evidence on a candidate rerun.

[SciPy's primary bootstrap documentation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html) defines replacement resampling of the original sample size, shared indices for paired arrays, percentile endpoints and bootstrap sample SD. It describes approximate coverage and warns that a normal approximation is not reliable in every case. Our algebraic checks follow directly from the estimands rather than copying repository formulas into the oracle.

**Protocol deviations and extensions:** the initial PR217 paired call failed because that revision lacks `observation_end`; the public adapter now checks the signature and omits that argument only there. The failed console log is preserved. A scale of `1e307` produced nonfinite fixture inputs and was correctly rejected; the meaningful finite-input overflow check uses `1e306`. After initial findings, a separate extension added ratio simulations, higher bootstrap counts and a larger unbalanced sample. The oracle and preregistration were not amended. No previous simulation results were reused.

## What passed

| Capability | Independent expected result | Observed evidence |
|---|---|---|
| Mean arm contrast | `[1,4,9,16]` vs `[2,5,10,17]`: treatment minus control = 1 | Both public scorecards agree; small-support inference is flagged |
| Ratio-of-sums | Control 17/6 = 2.833333; treatment 22/6 = 3.666667; delta 0.833333 | PR220 scorecard and native API agree, retaining numerator contribution with unit denominator zero |
| Mean-of-ratios | Explicit positive-denominator eligibility gives 2.5 vs 4.166667; delta 1.666667 | PR220 native API agrees and differs appropriately from ratio-of-sums |
| Literal whole-unit expansion | Recompute each statistic on repeated unit identities implied by count weights | Maximum absolute difference 1.33e-15 across 4,999 draws in each version |
| Repeated dependent events | Splitting each unit's contribution into seven events must preserve unit sum and uncertainty | All reported values, CI endpoints and p-values match the one-event data |
| Complete paired periods | Shared unit draws preserve pre/post covariance | PR220 public bootstrap SD 0.175842; independent literal SD 0.177288; analytic paired SE 0.180751; incorrect independent-period SE 8.247516 |
| Numeric rescaling | Rescale means and intervals; preserve ratio and standardized evidence | PR220 passes from `1e-200` to `1e200` on these fixtures; p-value 0.698767 for scaled mean in each case |
| Zero/sparse denominators | Undefined arm or undefined replicate must not become an observed zero | PR220 reports missing point estimates / `unstable_denominator` or `invalid_replicates`; native rejects matching-event missing values |
| Explicit weight scheme | Multinomial fixes the arm sample size; Poisson permits random total count | Both revisions expose explicit choices; no automatic size-dependent switch was found |

Raw fixtures, public returns, inputs and draws are saved under the revision prefix in `*_fixtures.json`, `*_input.csv`, `*_draws.npz`, `*_paired.json` and `*_kernel.json`. Using the kernel's own sampled counts for the algebraic equivalence check is an intentional adapter test; the independently seeded literal distribution check is saved separately.

## Findings and exact reproductions

### R03-01 — P1 historical defect, fixed in PR220: ratio missingness loses joint eligibility

**Public reproduction:** `score(c, t, kind='ratio')` from `run_review.py`, which calls `Eventstream.get_ab_test` and `get_ab_bootstrap` with `column_mean(num)` divided by `column_mean(den)` on one event per assigned unit.

```python
c = [[1,1], [99,float('nan')], [float('nan'),99], [8,2], [4,2]]
t = [[2,1], [777,float('nan')], [float('nan'),77], [12,2], [6,2]]
```

Expected after joint eligibility: control `13/5 = 2.6`, treatment `20/5 = 4.0`, delta `1.4`. PR217 reports control `1.076923`, treatment `9.719512`, delta `8.642589`. Numerators and denominators were separately zero-filled before reaching the otherwise jointly handling bootstrap kernel. PR220 returns the correct points; native `experiment_data` explicitly rejects these missing matching-event amounts rather than silently imputing them.

**Location:** PR217 `src/retentioneering/tools/ab_test.py:1071–1081`; PR220 repaired logic at `tools/ab_test.py:1106–1133`. **Scope:** value-based ratio components with numerator-only or denominator-only observations; not ordinary complete event sums. **Remedy/regression:** preserve PR220's joint eligibility mask; assert the three expected points on this independently authored fixture. This is a real estimator implementation defect in PR217 and must not be reintroduced while extracting features.

### R03-02 — P2 remaining implementation/diagnostic defect: representable results lost to intermediate overflow

**Public reproduction:** `c = arange(1.,41.) * 1e306`, `t = arange(2.,42.) * 1e306`; run the bootstrap scorecard and native mean adapter. Every input is finite. Expected means are `2.05e307` and `2.15e307`, with finite delta about `1e306`. Both scorecards lose the result; PR220 reports `support_warning='no_data'`. Native PR220 reports null point estimates and `undefined_denominator`, although denominators are exactly 40.

For the ratio variant, pair each numerator with `(arange + 3) * 1e306`. Expected ratios remain `0.8723404255` and `0.8775510204`, with delta `0.005210594876`; these are lost as well. At scale `1e200`, the same estimands and evidence work. Direct kernel resampling confirms all 999 draws become invalid at `1e306`, isolating the issue from the UI.

**Locations:** PR220 `stats/resampling.py:31` (`weights @ values`), `stats/experiments.py:92` and `107–110` (unscaled sums), `stats/experiments.py:75–80` (nonfinite point mislabeled as denominator failure), scorecard `tools/ab_test.py:2219–2222`. PR217 has analogous unscaled sum code.

**Scope:** extreme finite numeric magnitudes; no evidence of a failure at ordinary monetary scales. **Remedy:** scale contribution columns before resampling/aggregation and reconstruct representable statistics, or validate arithmetic overflow and give a distinct explicit error. A narrow safe release can enforce a supported numeric domain; it need not promise arbitrary IEEE-754 range. **Regression:** the same raw fixture must either retain the finite oracle point or reject clearly as numeric overflow, never silently claim no data/zero denominator. No source fix was attempted by this reviewer.

### R03-03 — P2 contract/estimand limitation: common roster is not necessarily common observed pairs

**Public reproduction:** repeat each of these three row pairs ten times per arm, supply all 30 assignments from before the common window, request daily `column_mean(num)` and `get_ab_bootstrap(..., contrast={'change':(['2026-01-02'],['2026-01-01'])})`:

```python
c = [[1,2], [100,float('nan')], [float('nan'),100]] * 10
t = [[2,3], [200,float('nan')], [float('nan'),400]] * 10
```

The mean of each arm's **complete paired changes** is 1, so its treatment effect is 0. The API instead computes period-specific eligible means, giving difference-in-changes 100. The PR220 public bootstrap interval is `[14.1496, 176.1050]` over 1,999 finite draws. This is correct resampling for the latter conditional estimand; it is not an estimate of the complete-pair mean change. The documentation's assertion that contrasts measure change within the same paths is too broad for missing value metrics.

**Locations:** PR220 `tools/ab_test.py:2060–2079` intersects period population masks, not finite outcome eligibility; `eventstream/eventstream.py:922–931` describes the same-path contrast. PR217 behaves similarly. **Remedy:** either label this explicitly as difference of period-specific conditional means and expose eligibility counts, or require common outcome eligibility for a paired-mean contract. Do not silently change ratio-of-sums estimands by dropping legitimate zero denominators. **Safe-bet restriction:** defer missing-value/conditional period contrasts or enforce complete eligible vectors. Complete pre/post outcomes passed the independent pairing check.

### R03-04 — method limitation, with existing disclosure: percentile CI and normal p-value differ

PR220's scorecard uses percentile intervals but `2*normal.sf(abs(delta)/bootstrap_SD)` for p-values; PR217 does the same for pointwise intervals. These are separately valid approximations under appropriate conditions, and need not invert one another. PR220 also computes adjusted bootstrap intervals from percentiles, while PR217 uses a normal interval for Bonferroni. Source: PR220 `tools/ab_test.py:2229–2237`; PR217 `tools/ab_test.py:2032–2045`.

The fresh 1,000-trial unbalanced lognormal null produced 67 disagreements between p-value rejection and percentile interval exclusion. This is not an arithmetic bug. A separately saved public ratio fixture (`baseline220_disagreement.json`) reports CI `[0.04299, 7.11579]`, p `0.054974` and verdict `inconclusive`; its reading **explicitly says that the test and interval disagree**, and the row exposes `ci_basis`/`p_basis`. This existing disclosure is a positive finding, not an omitted warning. Preserve those labels and the decision basis; do not state that a percentile CI is the inversion of this test. The separate argument for restricting automatic ratio/heavy-tail inference is the measured finite-sample calibration below, rather than the mere fact that two approximations can disagree.

### R03-05 — confirmed PR220 improvements and conservative availability boundaries

PR217's ordinary `nanstd` underflows at scale `1e-200` and overflows at `1e200`: bootstrap p-values become unavailable and 1.0, respectively, for a scale-invariant statistical question. PR220's stable SD returns 0.698767 in both cases. PR217's percentile and SE helpers also discard undefined draws; with one invalid among 999 draws they still produce an interval/p-value. PR220 returns unavailable for the same input. Preserve these improvements.

At two units per arm, explicit multinomial resampling gives all 999 defined draws; Poisson weights can give zero total count, and PR220 correctly withholds its interval. This is the consequence of different finite-sample procedures, not a hidden algorithm switch. PR220's native API withholds contrast intervals when either arm is empirically constant; the scorecard withholds when both are constant but can infer against one constant arm. The stricter native policy is conservative, not a mathematical requirement that every one-constant-arm comparison is invalid. The distinction should remain documented rather than being portrayed as a universal theorem.

Public availability checks in `baseline220_availability.json` confirm that the scorecard rejects B=99 and accepts B=100; native analysis without bootstrap returns `not_requested`. Native B=2 returns an `ok` interval and metadata warning `low_resample_count`. This is an explicitly permitted diagnostic setting, not evidence of a reliable 95% interval with two draws. A conservative release should keep such runs visibly diagnostic or require a production minimum; warning propagation to widgets is outside this role's UI audit.

## Fresh simulation evidence

Primary simulation seed `31416129` (`SEED + 202`); 1,000 independent trials per condition; 399 bootstrap replicates per trial; per-trial resampling seed `31425927 + trial`; 95% percentile CIs and two-sided normal-bootstrap p-values at alpha .05. Both arms have identical null populations. Data generation and every trial are preserved in `run_review.py`, `*_simulation_raw.json` and CSV. These simulations call the exported resampling engine; 15 additional end-to-end public scorecards bridge the first five datasets in each nonpaired condition. They are **not** a claim that every trial traversed Eventstream or native `experiment_data` availability rules.

| PR220 condition | Arm sizes | Available CIs | CI coverage among available (95% MC Wilson interval) | Null rejection (95% MC interval) |
|---|---:|---:|---:|---:|
| Independent Normal(10,1) means | 40 / 60 | 1000/1000 | 949/1000 = 94.9% [93.36%,96.10%] | 52/1000 = 5.2% [3.99%,6.76%] |
| Independent Lognormal(0,2) means | 15 / 150 | 1000/1000 | 726/1000 = 72.6% [69.75%,75.27%] | 217/1000 = 21.7% [19.26%,24.36%] |
| `1 + 100*Bernoulli(.01)` means | 30 / 90 | 706/1000 | 691/706 = 97.88% [96.52%,98.71%] | 11/1000 = 1.1% [0.62%,1.96%] |
| Paired normal changes with large shared latent level | 40 / 60 | 1000/1000 | 946/1000 = 94.6% [93.02%,95.84%] | 50/1000 = 5.0% [3.81%,6.53%] |

The rare-mixture case has 294 unavailable CIs, largely when the empirical sample is constant. Unavailable intervals are not counted as successes or described as nominal-coverage failures; the table separates availability from conditional coverage. PR217's corresponding coverage is 94.9%, 72.5%, 98.7% and 94.8%, with all rare-mixture intervals returned, including degenerate point intervals. Detailed PR217 rejection counts and uncertainty are saved in its summary JSON.

The lognormal population has finite moments; the difficulty here is finite-sample skew and an unbalanced design, not an assumption of infinite variance. More resamples reduce bootstrap Monte Carlo noise but cannot supply tail observations absent from the experimental sample. Passing the normal and paired cases supports those tested designs, without a universal finite-sample guarantee. The completed [extension](EXTENSION.md) adds 3,000 fresh datasets: regular ratios calibrate at 95.2–95.3% coverage; skew ratios remain at 73.2–73.4% when B increases from 399 to 1999; even a skew-mean sample of 50/500 gives 82.7% coverage. All denominators in those ratio simulations are strictly positive.

## Enforceable release boundary

| Capability | Recommendation | Reason |
|---|---|---|
| Complete unit means / ratio-of-sums / explicit mean-of-ratios | Retain descriptive arithmetic | Matches independent estimands; eligible population and zero-denominator rules must be visible |
| Whole-unit resampling and complete paired vectors | Retain as an explicit advanced method | Algebraic and distribution checks pass; units and full contribution vectors stay together |
| Invalid denominator / degenerate inference | Retain PR220 availability policy | Prevents silently conditioning on valid draws and fabricated precision |
| Means with declared independent units and a suitable analysis plan | Retain explicit, labeled inference if desired | Tested regular cases calibrate; no evidence justifies a categorical ban on all nonbinary inference |
| Automatic bootstrap test/winner for arbitrary ratio or heavy-tail outcomes | Restrict or defer in a conservative release | Concrete finite-variance designs have high type-I error; an observed-support threshold alone cannot establish tail regularity |
| Missing/conditional period contrasts called paired changes | Defer unless common eligibility is enforced or estimand is relabeled | Fixed roster does not ensure the same observed measurements in each period |
| Extreme accepted values overflowing internally | Fix or explicitly reject | Current labels say no data/undefined denominator rather than numeric overflow |
| Poisson resampling | Keep explicit and labeled, if exposed | Different finite-sample method; invalid draws require the same availability checks |

These recommendations do not require impossible universal coverage. They separate correct estimators with stated assumptions, actual implementation defects, and claims not supported by the tested design. A broader explicit advanced API can be legitimate with accurate method/population disclosure; a narrow safe-bet default can reasonably defer inference where no such contract is available.

## Reproduction

Run from `/Users/maximgodzi/Documents/ChatGPT/rete_presentation_case`:

```sh
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/03_resampling/run_review.py --label baseline220 --mode all --trials 1000
PYTHONPATH=.work/ab-safe-bet/pr217/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/03_resampling/run_review.py --label pr217 --mode all --trials 1000
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/03_resampling/extended_review.py --label baseline220
```

For a candidate rerun, change only `PYTHONPATH` and `--label candidate`; the frozen oracle remains unchanged. `command_log.txt` records actual runs. `evidence_manifest.json` hashes every role artifact except itself, and `REPORT.html` provides a standalone readable version. Findings need a second reviewer's reproduction before treating any proposed patch as verified.
