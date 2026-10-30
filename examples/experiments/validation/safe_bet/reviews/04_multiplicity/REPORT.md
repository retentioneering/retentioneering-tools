# Role 4 — multiplicity, power, heterogeneity and allocation

**Initial independent review, 2026-10-30.** The baseline's Holm and Bonferroni arithmetic, planned-family handling and default cross-metric scope pass the preregistered numerical checks. These are retainable for prespecified fixed-horizon families of valid p-values. BH, power/MDE, simultaneous intervals, heterogeneity diagnostics and Pearson SRM need the restrictions below. There are two reproduced public API defects, two misleading public claims, a missing BH assumption, and one isolated primitive defect. No source files were edited.

## Independence and provenance

Only `PROTOCOL.md` and the assigned task were read before preregistration. `PRE-REGISTRATION.md` and `oracle.py` were written and hashed before candidate source or test inspection. No CHECKPOINT or peer result was read. The frozen oracle was never amended. After unblinding, the adapter added interface-specific fixtures and the exact dependence example, without importing candidate code into the oracle. This is staged oracle-first review, not a formal blinded study.

| Item | Frozen identity |
|---|---|
| baseline220 | `38cbe91d5b315584575b132a596890e69eb210aa` |
| PR217 | `7e61d997bcbd9bae9f1571ba3521a85fcb59f751` |
| Preregistration SHA256 | `962c4105915e600f42743a13bebc699f1246a298d1537863c467252ac9dbf055` |
| Oracle SHA256 | `68bcad39632fe9ff1581477379617b5630453ec9dc9c3e1dba91886de51d4c37` |
| Runtime | Python in `.work/ab-integrated-wheel-env/bin/python`; NumPy 2.5.4, pandas 3.1.0, SciPy 1.18.1 |

The configurable [runner](run_audit.py) puts the selected source's `src` first on the import path and asserts that **every imported retentioneering Python module** belongs to that source. Each output directory contains `provenance.json` with imported paths and SHA256, versions, command arguments, oracle and runner hashes. Original first-run code, logs and provenance are preserved with `v1` names. The first fixed-calendar adapter mistakenly called `.to_dict()` on an already serialized public result; this was corrected before the final run. It was an audit-adapter error, not a library finding.

## Reproduction

From the workspace root:

```sh
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/04_multiplicity/run_audit.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/04_multiplicity/baseline220 --simulation-reps 100000
PYTHONPATH=.work/ab-safe-bet/pr217/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/04_multiplicity/run_audit.py --source .work/ab-safe-bet/pr217 --output output/AB_Safe_Bet_Review/roles/04_multiplicity/pr217 --simulation-reps 100000
```

All synthetic event and assignment CSVs used for the main family, period and mean fixtures are saved with results. The binary fixture constructor, exact counts, all further synthetic inputs and their expectations are in the executable runner. No bundled sample dataset or candidate test expectation was reused.

Final baseline checks: **426/427** pass; the one failed invariant is nonnegative MDE at an accepted power. PR217: **210/426** pass; most failures are intentionally the planned-missing-family oracle against its documented available-only policy, plus invalid primitive inputs, default scope and negative MDE. These counts are bookkeeping, not a statistical coverage certificate. Presentation claims, limitations and exceptions are assessed separately below.

## Confirmed findings

### F04-01 — P2: accepted power can produce a negative absolute MDE

**Type:** implementation/configuration defect. **Present:** baseline220 and PR217.

Public reproduction uses 100 independent users per arm, 30 treatment conversions and 17 control conversions, then:

```python
stream.get_ab_test(("arm", "treatment", "control"),
    metrics=[{"name": "conversion", "metric": "has_event",
              "metric_args": {"event": "conversion"}}], power=.01)
```

The public parameter validator accepts any `0 < power < 1`. The result is `mde=-0.021969289677944145`, `rel_mde=-0.12923111575261262`. Direct `mde_from_se(1, .05, .01)` returns about −0.366384. An absolute detectable effect cannot be negative. For a two-sided test whose null rejection probability is alpha, a target below alpha is already reached at zero; alternatively such low planning powers should be rejected under a documented domain restriction.

**Cause/location:** `stats/primitives.py:_mde_factor` (line 254) adds the normal quantiles without a nonnegative-domain condition; `tools/ab_test.py:ABTest.from_stream` (line 1546) accepts low power; `row_reading` later takes the absolute value for display, which cannot repair the numeric result.

**Remedy:** either explicitly restrict supported planning power (for example power at least 0.5), or invert a clearly defined two-sided power model and handle target power at/below alpha as zero. Do not silently take an absolute value. Preserve validation for nonfinite inputs.

**Regression:** a public call with `.01` must reject clearly or return a mathematically defined nonnegative result; ordinary `.8/.9/.95` cases must remain consistent with the declared approximate model. Evidence: `baseline220/public_low_power.json`, `mde_primitives.json`, and `checks.json`.

### F04-02 — P2: SRM cannot diagnose a planned arm with zero observations

**Type:** public API defect. **Present:** baseline220 and PR217.

Create an assignment roster with 100 control users and no treatment users, and call:

```python
stream.get_sample_ratio(assignment=roster,
    expected_shares={"treatment": .5, "control": .5})
```

Actual: `InvalidParameterError: Invalid value 'treatment' for parameter 'expected_shares'. Allowed values: ['control']`.

Expected: the explicit design defines both categories; the absent category receives zero observations. The Pearson statistic is 100, df=1, p≈1.52397e−23, which flags mismatch. Both expected counts are 50, so this example is not the sparse-expected-count limitation. The existing `_counts_table` already reindexes missing levels with zero, but `_levels_and_shares` rejects the missing label first.

**Location:** `tools/sample_ratio.py:SampleRatio._levels_and_shares`, lines 371–378; public `SampleRatio.fit` line 313. **Remedy:** distinguish a caller-declared expected design from observed categories and allow explicit positive-probability design arms with zero observations. Retain validation of accidental/conflicting labels elsewhere.

**Regression:** `[0,100]` against `[.5,.5]` must compute, and a zero observed arm in a multiarm design must retain the full positive-support df. Evidence: `baseline220/exceptions.json`, `srm_absent_arm_expected.json`; executable fixture `public_srm_absent_arm`.

### F04-03 — P2: the Bonferroni CI/verdict agreement claim is false for binary rows

**Type:** documentation/interpretation defect; **the two calculations are individually intentional methods**, not evidence that either formula was transcribed incorrectly. **Present:** baseline220 and PR217.

With 15 users per arm, 9 treatment versus 3 control conversions, declare two binary metrics and `correction="bonferroni", correction_scope="all"`. The runner uses two named copies of the same conversion, which is a valid conservative family for this demonstration. Public output:

| Quantity | Actual |
|---|---:|
| Pooled-z raw p | 0.02534731867746828 |
| Bonferroni p | 0.05069463735493656 |
| Verdict | inconclusive |
| Adjusted Newcombe low | **0.003081619972102101** |
| Adjusted Newcombe high | 0.6614398272728212 |

The adjusted interval excludes zero, but the adjusted test does not reject. The guide explicitly presents Bonferroni intervals as agreeing with the verdict (`docs/guide/ab-testing.md:150`). That statement is contradicted by the public result. Newcombe's hybrid interval is not the inversion of this pooled-z test. The runtime reading does disclose the disagreement, which is good.

**Remedy:** remove the unconditional duality claim; identify method and nominal adjusted level. If exact test/CI duality is a product requirement, use a matched test and interval, with separate statistical validation. A Bonferroni nominal adjustment does not make approximate single-comparison intervals exact simultaneous coverage guarantees.

**Regression:** preserve the 9/15 versus 3/15 fixture and require honest wording. Do not force the verdict to follow the interval. Evidence: `binary_ci_test_disagreements.json`, `public_bonferroni_ci_disagreement.json`.

### F04-04 — P2: public MDE prose promises actual power beyond its normal approximation

**Type:** presentation claim needing restriction; **not an arithmetic defect** at conventional powers. **Present:** baseline220 and PR217.

The normal-approximation formula `(z(1-alpha/2)+z(power))*SE` matches the independent oracle for all checked conventional powers, allocations and baseline rates. At alpha=.05 and power=.8, the fixed-SE normal model gives essentially 80% power. But the scorecard also applies it to a small-sample Welch test and bootstrap-SE estimates, and calls it what the design detects. The guide says that this is the effect this test detects with 80% power (`docs/guide/ab-testing.md:137–148`).

A public count/value fixture has five observations in each arm, both mean 10 and sample SD 1. It passes default pooled nonzero support 10. The scorecard reports `mde=1.77187806965932`; its reading says the design detects 1.77 and recommends about 12.6 times the paths for a target of .5. For independent Normal(0,1) and Normal(1.771878...,1) populations with n=5 each, a fresh Monte Carlo Welch test rejects only **66.257%**, not 80%. The 95% Monte Carlo interval is [65.964%,66.550%] (100,000 replications, seed 4072027). This even grants the plug-in formula the true unit variance; it is not uncertainty in the baseline variance alone.

| n per arm | Effect from normal formula | Actual Welch power | 95% Monte Carlo interval |
|---:|---:|---:|---:|
| 3 | 2.287485 | .45763 | [.45454,.46072] |
| 5 | 1.771878 | .66257 | [.65964,.66550] |
| 10 | 1.252907 | .75294 | [.75027,.75561] |
| 30 | .723366 | .78890 | [.78637,.79143] |
| 100 | .396204 | .79635 | [.79385,.79885] |

**Remedy:** retain as an explicitly approximate, variance-assumption-dependent planning diagnostic; label the numerical method and avoid promising attained power or definitive future sample sizes. For actual test-specific power use noncentral-t/validated simulation as appropriate. Binary MDE uses the observed pooled rate and local null variance, not an alternative-specific binary power calculation; bounded rates can also make the suggested effect infeasible. Holm/BH use `alpha/m` for this diagnostic, a conservative Bonferroni planning threshold rather than the actual rank-dependent rejection threshold. Per-comparison 80% power does not mean an 80% probability of detecting every effect in the family.

**Location:** `stats/primitives.py:mde_proportion/mde_mean/mde_from_se`; `tools/ab_test.py:_test_cell`, `row_reading:2841`, `_paths_needed:2635`; guide lines 137–150. **Regression:** test honest metadata/reading for the small-n fixture. Do not replace a correct approximation with an unvalidated claimed exact method. Evidence: `public_small_welch.json`, `fixture_small_welch_events.csv`, `welch_mde_power_simulation.csv`.

### F04-05 — P2: BH's FDR claim needs a dependence condition

**Type:** missing method assumption / restrict capability. **Present:** baseline220 and PR217.

The BH arithmetic is correct. The primitive and guide recommend it for many exploratory rows without stating that independence or suitable positive dependence is required. Independence of randomized users does not establish the required dependence property of all metric/stratum/time test statistics.

An exact all-null example uses four possible vectors with valid super-uniform marginal p-values:

| Probability | p1 | p2 | BH rejects any at .05? |
|---:|---:|---:|---|
| .024 | .024 | 1 | yes |
| .024 | 1 | .024 | yes |
| .024 | .048 | .048 | yes |
| .928 | 1 | 1 | no |

For each marginal, P(p≤.024)=.024 and P(p≤.048)=.048. BH FDR is exactly **.072**; Holm and Bonferroni FWER are .048. The candidate produces precisely these decisions. This is not a discovered implementation bug or a claim that the concrete event fixture has this joint distribution; it disproves an arbitrary-dependence guarantee.

**Remedy:** explicitly state the dependence assumptions and retain Holm/Bonferroni for arbitrary-dependence confirmatory families. An exploratory label does not repair invalid p-values from selected hypotheses. No change to the BH arithmetic is requested. **Location:** `stats/primitives.py:benjamini_hochberg:345`; guide lines 393–397. Evidence: `bh_dependence_exact.json`.

### F04-06 — P3: zero-probability categories give the wrong SRM primitive df

**Type:** isolated documented-primitive defect; public scorecard SRM adapter rejects zero expected shares, so it does not reach this branch. **Present:** baseline220 and PR217.

`srm_chi2([68,32,0], [.5,.5,0])` returns Q=12.96 with df=2 and p=.001533810679324463. The last category is deterministically zero under the null and should not add a degree of freedom: positive-support df=1 and p=.000318217180315068. At the common SRM alpha=.001, this flips the alarm. The primitive explicitly permits zero shares and has a zero/zero contribution special case, so this is within its own contract.

**Location:** `stats/primitives.py:srm_chi2` computes `dof=max(k-1,0)` before removing zero-probability categories. **Remedy:** either reject zero shares consistently at the primitive boundary, or compute df from positive-probability categories while still flagging any positive observation in an impossible category. **Regression:** zero/zero category should not change the result for the remaining categories. Evidence: `srm_primitives.json`.

## Retained behavior and restricted interpretations

### Planned families and PR217 comparison

The hand vector `[.01,.04,.03,.002]` gives:

| Procedure | Independently expected adjusted p-values |
|---|---|
| Bonferroni | [.04,.16,.12,.008] |
| Holm | [.03,.06,.06,.008] |
| BH | [.02,.04,.04,.008] |

Both sources pass the finite arithmetic, order, ties and boundary checks. Baseline also rejects negative, >1 and infinite p-values. PR217 accepts these at the primitive layer. Baseline includes missing entries in m; missing outputs remain missing. For `[.03,NaN]`, all three baseline procedures return `[.06,NaN]`, matching the planned-family oracle. PR217 returns `[.03,NaN]`, matching its explicitly available-only contract. This is a change in inferential scope rather than a mistake in the finite-step algorithm, and cannot be advertised as controlling a prespecified full family after outcome-dependent availability.

The public 30/100 versus 17/100 fixture has p=.03015700478. With a second declared binary metric that is constant one in both arms, baseline all-scope adjusts to **.06031400956**, MDE alpha=.025 and `inconclusive`; PR217 all-scope adjusts to **.03015700478**, MDE alpha=.05 and `increase`. Baseline's default is all declared rows; PR217's default is separate metric families, so even two available metric names yield distinct default verdicts. Explicit baseline `correction_scope="metric"` correctly returns independent metric families, but does not control picking a winner across them.

The 2-metric × 3-stratum × 2-period fixture produces 12 rows. Baseline `all` uses the full set of 12; `metric` uses six rows per metric. The p-value arithmetic agrees with the oracle in both cases. The labels expose the chosen scope. No correction spans separate calls or repeated stopping looks. The strict fixed-calendar adapter also retains declared unavailable tests: Fisher p=.04467093380 becomes .08934186760 when the other declared Welch test is unavailable. Its metadata explicitly says intervals are pointwise and exact allocation assumes independent Bernoulli assignment.

### Null simulation illustrates scope, without certifying real-data calibration

100,000 independent Uniform(0,1) null families, ten p-values each, seed 4072026:

| Decision rule | False-positive families | Rate | MC SE |
|---|---:|---:|---:|
| Ten separate singleton families, any rejection | 40,117 | .40117 | .001550 |
| One Bonferroni family of ten | 4,970 | .04970 | .000687 |
| One Holm family of ten | 4,970 | .04970 | .000687 |
| One BH family of ten, all null | 5,092 | .05092 | .000695 |
| Keep only smallest p, then available-only family | 40,117 | .40117 | .001550 |
| Keep only smallest p, retain planned m=10 | 4,970 | .04970 | .000687 |

The independent expectations are 1−.95^10≈.401263 for singleton-union/minimum-selection, 1−.995^10≈.048890 for Bonferroni/Holm under the global null, and .05 for BH. Full confidence intervals are in `null_simulation.csv`. This is an oracle-only explanatory simulation; it does not claim that the library's support filter literally selects the smallest p. It shows why changing the family after observing outcomes needs justification. No multiplicity procedure fixes nonuniform input p-values, selection bias, nonindependent users or optional stopping.

### Heterogeneity

The scalar Q fixture effects [0,1,2], variances [1,1,1] gives weighted mean1, Q2, df2, p=exp(−1), matching both sources. Baseline's scaled arithmetic also passes unit rescaling by 1e−150 and 1e150; PR217 happened to pass those chosen scales too. Three disjoint binary strata with treatment counts [39,25,15], control [25,25,25], n=80 per arm/stratum give Q=8.66208350, df2, p=.01315383731. All public values match the independently computed unpooled variances.

Stratum-versus-rest compares a stratum against the pooled other units with variance addition, and matches independent raw calculations: [.00923422136,.78133191505,.01370759253]. These are **unadjusted** p-values; Holm over these three would be [.02770266408,.78133191505,.02770266408]. The guide says unadjusted, so this is a retained diagnostic with a restriction rather than an arithmetic defect. Primary `p_value_adjusted` does not adjust these other hypotheses. Q is repeated on each stratum row for a given metric/period, not an additional independent test for each row. Multiple Q tests across metrics or periods remain unadjusted. A significant Q followed by unadjusted rest tests is not a general strong-FWER closed-testing procedure.

Retain Q as an asymptotic diagnostic for independent stratum estimates with adequate positive variance, and rest tests as explicitly unadjusted normal approximations. Support-based exclusion means the tested homogeneity claim is about the usable strata; it does not prove all planned strata share an effect. Significance in one stratum but not another is not a heterogeneity test. Pre-exposure status and randomization assumptions remain caller responsibilities beyond these numerical checks.

### Allocation/SRM

For counts [25,75] and configured shares [.25,.75], Q=0 and p=1; testing the same counts against [.5,.5] yields Q25. Public assignment counts include the declared roster and use expected shares correctly. Baseline outputs `sampling_assumption="independent_multinomial"` and `p_value_method="pearson_chi_square_asymptotic"`; fixed-calendar `experiment_data` uses an exact two-sided binomial probability with `scheme_assumption="independent_bernoulli"`. These are useful and honest distinctions. They do not certify randomization, loss-free logging, fixed-total/blocked allocation or post-exposure subsets.

**Sparse restriction:** with ten users, expected treatment probability .01 and observed treatment count2, the public Pearson check gives p=1.55460544e−9 and flags mismatch at .001. The exact two-sided binomial p is .00426620024. Exact enumeration of all possible assignments shows the Pearson alarm rejects under its own null with probability .00426620024, over four times nominal. This is a known asymptotic-method limitation, not incorrect Pearson arithmetic. Retain only with an adequate expected-count condition, visible warning/unavailable status, or a validated exact alternative for sparse two-arm checks. Existing method metadata alone does not quantify the sparse failure.

Split SRM p-values are Holm-adjusted within the requested strata or period table. The overall SRM p-value is separate; an alarm chosen from the union of overall, split analyses and repeated calls has no single alpha guarantee. A fixed 50/50 allocation scheme can make overall equal counts structural, so lack of an SRM alarm proves little. Evidence: `public_srm_unequal.json`, `public_srm_wrong_equal.json`, `public_srm_split.json`, `public_srm_sparse.json`, `srm_sparse_exact.json`.

## Function-by-function release matrix

| Function / adapter | Baseline decision | Evidence / required boundary |
|---|---|---|
| `primitives.holm` | Retain | Valid fixed family, planned NaNs retained; arbitrary dependence allowed when input p-values valid |
| `primitives.bonferroni` | Retain | Same boundary; correct family count and clipping |
| `primitives.benjamini_hochberg` | Restrict | Correct arithmetic; independence/suitable positive dependence and valid prespecified p-values required; FDR not general FWER |
| `experiments.adjust_pvalues`, `_tests` through `experiment_data` | Retain for declared methods | Public planned-unavailable fixture passes; strict fixed-horizon family only |
| `ABTest._adjust` through `get_ab_test` | Retain correction behavior | Default all rows; explicit metric families; unavailable hypotheses counted; verdict follows adjusted p |
| `ABTest._test_cell` adjusted binary CI | Restrict claim | Approximate Newcombe nominal Bonferroni interval; no pooled-z duality guarantee |
| `mde_proportion` | Restrict + repair low-power domain | Correct local normal formula, pooled baseline and feasibility limitations; F04-01 |
| `mde_mean` | Restrict + repair low-power domain | Common-SD normal planning formula; not finite-sample Welch power |
| `mde_from_se` | Restrict + repair low-power domain | Fixed-SE normal approximation; estimated/bootstrap SE adds assumptions |
| `row_reading`, `_paths_needed` | Restrict/repair wording | Approximate MDE and sample-size extrapolation; F04-04 |
| `primitives.cochran_q` | Retain as approximate diagnostic | Independent positive-variance strata; usable support subset; source and public counts checked |
| `ABTest._strata_columns` Q and rest tests | Restrict | Correct tested raw calculations; secondary hypotheses unadjusted across metrics/periods; no post-selection guarantee |
| `primitives.chi2_balance` | Restrict; indirect numerical check | Balanced-strata public p=1; general asymptotic independence table, sparse cells unvalidated in this role |
| `primitives.srm_chi2` | Retain positive-support arithmetic, repair zero support | F04-06; independent multinomial and adequate expected counts |
| `SampleRatio.fit/_levels_and_shares` | Repair then retain with restrictions | F04-02 absent planned arm; sparse warning/method restriction |
| `SampleRatio._split_tables` | Retain local family behavior | Holm within one split; overall and repeated calls separate |
| `experiment_data` exact allocation metadata | Retain under independent Bernoulli design | Equal allocation public exact check; source explicit non-50/50 probability; no fixed-total design claim |
| `ABTest` paired bootstrap contrast correction | Deferred to resampling reviewer | Source inspected: separate contrast family and pointwise CI label; not executed in this role |
| Frontend/MCP, conversion/funnel multiplicity | Deferred to their assigned reviewers | No claim of full adapter/UI coverage from this review |

## Recommended safe boundary

Keep baseline's planned all-row Holm behavior as default, with pointwise interval labeling and a prespecified fixed-horizon analysis. Keep Bonferroni as a nominal adjustment with honest interval method labels. Keep BH as a restricted exploratory procedure under stated dependence/selection assumptions. Present MDE as approximate planning information, repair negative outputs, and never interpret nonsignificance as practical equivalence or guardrail safety. Keep Q/rest and SRM as diagnostics with their own stated hypotheses and sampling assumptions. The current results do not certify arbitrary distributions, small samples, sparse allocation, adaptive selection, repeated looks, cluster randomization or simultaneous confidence for every displayed number.

## Primary methodological references

- [Holm (1979), A Simple Sequentially Rejective Multiple Test Procedure](https://www.ime.usp.br/~abe/lista/pdf4R8xPVzCnX.pdf): fixed-family step-down procedure and FWER.
- [Benjamini and Hochberg (1995), Controlling the False Discovery Rate](https://onlinelibrary.wiley.com/doi/10.1111/j.2517-6161.1995.tb02031.x): original independent-test FDR procedure.
- [R stats, p.adjust](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/p.adjust.html): declared family size can exceed observed p-values; missing/unobserved convention and dependence distinctions.
- [R stats, power.t.test](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/power.t.test.html): test-specific power calculations and explicit two-sided rejection convention.
- [SciPy, chisquare](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chisquare.html): independent multinomial model, k−1 degrees of freedom and sparse-frequency limitation.

The finite vectors, Q, SRM, normal power equation, dependence construction and simulation expectations above were derived independently from probability algebra. References constrain interpretation; candidate comments and candidate golden tests were not used as oracle values.
