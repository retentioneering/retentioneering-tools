# Role 5: graph and sequence audit of PR221

**Decision: defer the new inferential graph/step APIs and significance overlay from the conservative release.** The per-unit descriptive quantities passed independent checks, but default binary inference failed an imbalanced complete-null experiment and the ratio bootstrap produced a decisive verdict from floating-point noise. These are specific failures, not a claim that every result is wrong.

Review date: 2026-10-30. Target: `9d130f375b8a778a55e11de98b57d46ad082c55f`. Baseline comparison only: PR220 `38cbe91d5b315584575b132a596890e69eb210aa`. No target source was changed, no peer review or CHECKPOINT was read, and no subagents were used.

## Independence and evidence

`PRE-REGISTRATION.md` and the plain-Python `oracle.py` were written and hashed before source/API inspection. Oracle SHA256: `5f15b60e0aa35941aacee103303e04ac27f752dada100522d41fc0e8a378d219`; preregistration SHA256: `388cda923ff8521b7a83b389bcaf0ef3a5befdc5522e2edf844d5004091a4f47`. Both hashes were verified unchanged at completion.

The oracle counts directed consecutive edges independently for each randomized unit, edge reach, unique complete label paths, incoming/outgoing/total denominators, and step occupancy. The API's `unique_paths` means edge reach; it does not mean the number of distinct complete paths. Post-inspection adaptations are recorded in `AMENDMENTS.md`: synthetic start/end edges for nonempty paths, one-based steps, ended occupancy, and the API's implicit event-derived cutoff. The frozen oracle was not changed. The null stress experiment was specified after source inspection but before running it. Bootstrap arithmetic was independently computed using plain weighted sums, with the implementation's documented RNG child-seed convention adopted for exact reproducibility.

Runtime: Python 3.13.17, NumPy 2.5.4, pandas 3.1.0, SciPy 1.18.1, DuckDB 2.0.0. The environment's installed distribution metadata says retentioneering 5.3.0; the code actually imported was the frozen PR221 `src/` tree, not evidence of a released PR221 wheel. Full hashes and invocation are in `raw/manifest.json` and `EVIDENCE.sha256`.

Main harness: 1,398 checks, 1,396 passed. The two initial failed expectations concern the inferred unit guard below its ten-user threshold; they are retained in the raw evidence and classified below, not hidden or counted as arithmetic failures. Supplemental harness: 11 checks, 10 passed; its remaining failure is the constant-bootstrap defect. Repository tests: **39 passed, 3 slow tests deselected** (`pytest.log`). These passes are bounded evidence, not a coverage certificate.

## F1 — P1: pooled support does not validate a small-arm z-test

**Class:** inferential-validity defect in the accepted default domain, not a counting error and not an identified BH-dependence failure.

Public reproduction: construct 2 treatment and 1,000 control units, each with one event sampled independently from the same uniform distribution over 50 labels. Call `get_ab_steps(("arm", "treatment", "control"), max_steps=1)` and `get_ab_transitions(("arm", "treatment", "control"))`, with default `min_support=10`, correction BH and alpha 0.05. `run_audit.py` generates 200 experiments using `numpy.random.default_rng(51000 + seed)`, seed 0–199. All hypotheses are null. Hence the chance of any discovery is also the FDR for each screen.

| Scenario | API | Experiments with discoveries | Rate; Wilson 95% interval | Total discoveries |
| --- | --- | --- | --- | --- |
| 2 vs 1,000 units | steps | 200 / 200 | 100%; 98.12–100% | 396 |
| 2 vs 1,000 units | transitions | 200 / 200 | 100%; 98.12–100% | 792 |
| 250 vs 250 units | steps | 3 / 200 | 1.5%; 0.51–4.32% | 3 |
| 250 vs 250 units | transitions | 3 / 200 | 1.5%; 0.51–4.32% | 6 |

The transition screen has both start-to-event and event-to-end hypotheses, accounting for the duplicate discoveries. Outcomes across the two APIs are correlated; the 200-run intervals are computed separately, not by pooling them as 400 independent experiments. The balanced result does not establish general calibration.

A deterministic public reproduction is also saved: treatment A/B counts 1/1 and control A/B counts 50/950. `get_ab_steps(..., max_steps=1)` gives A's raw and BH-adjusted p about **0.0038208662**, `verdict="increase"`, and an empty support warning. The smaller arm has only **0.1018 expected A events** under the pooled null. An independent two-sided Fisher exact test for `[[1,1],[50,950]]` gives **0.0992540394**. The latter is a reference for this fixed table; the Monte Carlo null experiment is the stronger calibration evidence.

**Location:** `tools/ab_test.py:1645–1682`, especially pooled support at 1660–1667; `tools/ab_transitions.py:307–318` dispatches the admitted row to the z-test. The per-arm `q_support` flag exists but is not used to establish the validity of this binary screen. The public APIs do not require balanced allocation or a minimum small-arm size. NIST describes this normal approximation for reasonably large samples and identifies an exact test for small samples: [two-process proportions](https://itl.nist.gov/div898/handbook/prc/section3/prc33.htm).

**Remedy and regression:** use valid conditional/exact or randomized-unit permutation p-values for sparse binary tables, or enforce a documented expected-cell criterion and leave failing rows descriptive. Merely changing BH to Holm/Bonferroni cannot repair invalid marginal p-values. Re-run this null generator, balanced contexts and one-arm-only effects with adequate expected counts; report rates and Monte Carlo uncertainty. Preserve one-arm-only effect detection where inferential conditions hold.

Evidence: `raw/simulation_runs.{json,csv}`, `raw/simulation_summary.json`, seed-0 datasets/results, and `raw_extra/fixed_small_arm_*`. Every simulated dataset is reconstructible from the saved generator, seed schedule, arm sizes and versions; raw seed-0 datasets and every run's outcome/family size are saved.

## F2 — P1: constant ratio bootstrap turns roundoff into certainty

**Class:** numerical implementation defect in degeneracy handling.

Public reproduction: 30 treatment units all have path `A,B,A,C,A,C`; 30 control units all have path `A,B,A,C`. Call `get_ab_transitions(diff, "proba_out", edges=[("A", "B")])` with defaults. Every treatment unit contributes numerator/denominator 1/3; every control unit contributes 1/2. Every within-arm multinomial resample therefore has the same exact contrast, −1/6.

Actual output: `delta_se=2.77694638204588e-17`, `p_value=0`, `verdict="decrease"`, and a zero-width interval at −1/6. Its reading claims the interval excludes zero. A tiny rounding residual is being interpreted as estimated sampling spread. The expected result under the implementation's own stated constant-statistic convention is descriptive estimates with no inferential p-value/interval/MDE, not a rounded-zero p-value. This does not claim the point-estimate difference is false; it identifies unsupported uncertainty.

**Location:** `tools/_bootstrap.py:364–377` uses `np.nanstd`; `tools/ab_transitions.py:348–374`, especially `constant = se == 0` at 357, trusts exact numerical zero. The sparse numerator/denominator arithmetic itself matched independent bootstrap calculations on a nondegenerate fixture.

**Remedy and regression:** explicitly detect invariant arm statistics or identical replicate values before floating variance calculation, and consistently mask p, intervals, SE, MDE and inferential verdicts for degeneracy. Do not use a broad arbitrary epsilon that hides real small variation. Include nonbinary fractions, unequal arm constants, different replicate counts and both resampling modes. `run_diagnostics.py` preserves a failing-before check; `raw_extra/constant_ratio*` contains all inputs and actual results.

## F3 — P2: graph overlay compatibility does not bind the data

**Class:** API validation gap; it becomes user misuse if callers deliberately attach a frame from a different population, but the current compatibility check does not detect that mistake.

The public graph constructor correctly preserves original `n_tested` and `n_rows` when passed a filtered result frame. It rejects swapped arm labels, a mismatched edge weight, and a path-pattern graph. However, a result calculated from 20 treatment `A,B` and 20 control `A,C` paths is accepted on a second stream with B/C swapped. The first frame has A→B delta **+1**; the current stream has true API delta **−1**. The constructor accepts the stale payload with `error=""`.

**Location:** `tools/ab_transitions.py:_attrs` records labels/grain/window but no source/population identity; `transitions_payload` around 1000–1057 checks labels and weight; `widgets/transition_graph.py:152–178` additionally checks path grain, not the stream/window population.

**Test limit:** the source checkout has no built `widget.js`. Direct construction first failed with `FileNotFoundError`, saved in `raw/observations.json`. Supplemental construction uses a local stub JS resource through an in-memory `_esm` shim. It executes the real public Python constructor, validation and payload path, without changing sources. **No browser rendering or bundled-JS correctness is claimed.**

**Remedy and regression:** bind overlays to source/preprocessing/population/window identity, or require an explicit and visible mismatch acknowledgement. Preserve filtering of output rows without recomputing its original family. Test filtered frames, filtered streams, changed event values, horizon overlays, and assignment-table arm membership. Evidence: `raw_extra/stale_overlay.json`, `overlay_original_frame.json`, `overlay_changed_actual_frame.json`, `filtered_frame_overlay.json`.

## Estimands, assumptions and tested limits

**Descriptive arithmetic retained in principle.** All five edge weights match independent per-unit calculations on repeated loops, unequal lengths, absent edges in an arm, nonempty and silent roster units, and half-open horizons. Synthetic start/end edges are included for active paths; silent users contribute no transition. `unique_paths` divides binary edge reach by all analyzed units; `avg_per_path` divides occurrence counts by that population. Probability weights are **ratios of pooled sums**, not averages of each reached user's ratio. Sources, targets and all transitions supply the correct denominators. A missing source in one arm yields NaN for its conditional probability rather than an invented zero. Step occupancies plus ended mass sum to one at each tested position. Completely silent analyzed windows produced an empty transition table and ended step cells as appropriate. A truly empty raw stream with a populated assignment roster is rejected by the assignment-overlap guard; that is a restrictive contract, not a wrong estimate.

**Count and bootstrap arithmetic.** An independent per-unit Welch reference matched the public count SE/p. For nondegenerate `proba_out`, 1,200 multinomial and Poisson replicates matched independently looped path-weighted sums in SE, percentile endpoints and valid count. Units were resampled jointly, preserving repeated-event clustering and numerator/denominator covariance. This validates arithmetic on the fixture, not asymptotic calibration for every heavy tail, sparse source, ratio denominator or Poisson realization. A sparse-source fixture with one reached user per arm had only 391/1,000 valid multinomial contrasts; p/CI/SE were absent and `support_warning="invalid_replicates"`. Its machine verdict was `inconclusive`, while the text correctly said it was untested/descriptive; downstream clients must inspect finite p/status, not interpret that as evidence of no difference. PR221 tolerates up to 1% missing-denominator replicates, conditioning summaries on valid draws; this is a policy assumption, not a universal sparse-bootstrap guarantee.

**Conditional interpretation.** `proba_out`, `proba_in`, and `share_of_total` carry `conditional=True` and a clear post-treatment behavior warning. Reach-conditioned quantities do not identify the effect among a common fixed set of reached people. They are descriptive arm-specific pooled ratios; treatment can change both the numerator and who/what contributes to the denominator. `share_of_total` is compositional, so another transition changing can move a share. Even binary reach and step occupancies are causal effects only with a proper randomized roster, consistent measurement/horizon, and no informative filtering of units.

**Support and family.** Declared absent edges receive explicit zero estimates/no-data rows; duplicate declared edges are deduplicated; unsupported/no-p rows are excluded from `n_tested`. Returned `correction_family` and readings disclose that tested family. The family is selected from observed behavior/support, and it is not a claim of simultaneous inference over all possible transitions, multiple weights, alternative horizons, or repeated searches. Pooled support selection is invariant to arm labels under a sharp randomized null, so it is not automatically equivalent to selecting on the largest treatment effect. A conditional permutation analysis can exploit that fact. The present z/normal/bootstrap p-values and multi-weight search still need their own validity argument.

**BH dependence.** Edge indicators share paths and step cells are mutually exclusive at a position; shared denominators add further dependence. A general PRDS/independence justification is not supplied. The [Benjamini–Yekutieli paper](https://www.math.tau.ac.il/~ybenja/depApr27.pdf) establishes BH control under specified positive dependence and gives a conservative modification for other dependence. This review does not prove BH fails solely because graph cells are dependent. It does reject an unconditional FDR-control claim: F1 already shows failure from invalid marginal p-values. After repairing p-values, arbitrary-dependence protection or an explicit restricted claim is still required. Predeclared edges do not by themselves fix sparse inference, unit grain, window selection, or dependence.

**Unit grain.** The main tiny fixture allowed a finer session grain without raising, producing the two retained failed expectations. Source inspection explains the heuristic: inferred segment-unit enforcement requires at least ten coarser units with several finer paths. Supplemental tests with 24 multi-session users correctly raised in both APIs, as did an explicitly user-indexed assignment table analyzed at session grain. Thus do not describe inferred grain protection as exhaustive; use a named randomized-unit roster. No independent assignment mechanism can be recovered solely from a constant event column.

**Cutoff.** Horizon boundaries/pre-assignment exclusions agree with the oracle under a fixed supplied marker. But PR221 derives maturity from the maximum timestamp of the entire stream. With unchanged 12+12 assigned paths at times 1 and 2 and a 3-second horizon, both APIs raise for no mature arm. Adding one unrelated, unassigned event at time 3 makes all 24 units eligible. This follows the documented contract but does not establish logging completeness. There is no explicit `observation_end` on these graph/step APIs. Evidence: `raw_extra/cutoff_marker=*` and `raw_extra/observations.json`.

## Safe-release matrix and lineage

| Capability | Recommendation | Reason |
| --- | --- | --- |
| Per-unit edge counts/reach and step occupancy semantics | Retain as validated descriptive building blocks | Independent arithmetic matches; preserve denominators and boundaries |
| New default binary edge/step inference | Defer | F1 accepted small-arm null domain gives false discoveries in 200/200 runs |
| Conditional ratio inference | Defer as a safe default; descriptive ratios can be retained | F2 degeneracy plus sparse/bootstrap/conditioning assumptions |
| Count Welch inference | Restrict to established scorecard/design safeguards | Fixture arithmetic passed; no broad heavy-tail or adaptive-screen certificate |
| Filtered result-frame metadata | Retain design | Original family size correctly preserved |
| New significance overlay | Defer | F3 data identity gap; no built frontend tested here |
| Horizon and unit handling in an extraction | Require baseline safeguards | Inferred grain and last-event cutoff are incomplete |
| General BH/FDR-control or declared-family confirmatory claim | Defer | Valid p-values and dependence/selection/design conditions are necessary |

PR221 is stacked on PR217 and does not incorporate PR220. Comparing it with the candidate PR220 baseline reveals **lineage/extraction hazards, not changes that PR221 made to PR220**: PR221 lacks PR220's explicit observation cutoff/analysis-unit contract, changes the shared sparse-bootstrap invalid-draw threshold from PR220's zero to 1%, and uses a different constant-statistic policy. The source-only difference is saved as `baseline220_to_pr221_ab_test.diff`; no baseline was imported into the test run. A conservative extraction must not replace the baseline shared engine wholesale to acquire graph screens.

The smallest safe release decision is to defer PR221's new public inferential surfaces while retaining established descriptive graph/step tools. Any later inclusion needs a separate cross-reproduction, failing-before regressions, repaired null calibration, preserved PR220 population safeguards, wheel verification, and live browser/CI checks by their assigned reviewers. This role performed no extraction, repair, merge, or publication.

## Reproduce

See `COMMANDS.md`. `run_audit.py --source <checkout-or-src> --output <fresh-output-dir> --simulation-runs 200` and `run_diagnostics.py --source <checkout-or-src> --output <fresh-extra-dir>` accept a replacement source for future reruns. Preserve the original `raw/` and `raw_extra/` evidence. Full source hashes, raw input/output JSON/CSV, independent bootstrap contrasts, initial deviations, stderr/stdout logs and the repository test result accompany this report.

Untested scope includes built wheel behavior, real browser UI/export, live CI, arbitrary DAG/path dependence, broad timezone/tie policies, very large sparse-matrix memory/performance limits, and formal FDR/coverage guarantees over all accepted inputs.
