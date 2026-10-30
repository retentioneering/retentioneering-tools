# Role 04 cross-review — inference boundaries

2026-10-30. This follows the frozen initial role-04 review. The initial preregistration, oracle, reports and evidence were not modified. This phase explicitly read the initial reports of roles 01 (binary), 03 (resampling) and 05 (graphs), then made fresh reproductions. It is a cross-review, not a new blinded audit. No peer code, seed or generated data was reused.

## Conclusions

The proposed binary default of Fisher's exact test with a conservative Clopper–Pearson difference interval is a defensible narrow policy for a prespecified independent-unit binary experiment. The test and interval are different constructions and can disagree. The interval can be built conservatively by allocating the tail error across the two arm intervals and subtracting endpoints; the union bound supplies the coverage rationale. Family-wide interval protection requires allocating error across all relevant comparisons too. This does not validate post-exposure populations, optional stopping, adaptive hypothesis selection or dependent randomization units.

Making nonbinary `auto` descriptive is also a defensible conservative default. It should not be described as proving that all Welch or bootstrap inference is invalid. Explicit, labeled approximation methods remain useful for suitable planned designs; the role-03 regular and skew examples, and the new runs here, show why a universal guarantee is not justified. The caller needs an estimand and sampling design that support the chosen approximation.

Role-05's caution that changing BH to Holm cannot repair invalid marginal p-values is independently substantiated below. This cross-review did not execute the PR221 graph APIs, constant-ratio reproduction or stale-overlay fixture, so it does not independently certify those other findings.

## Fresh public small-arm reproduction

The configurable `cross_reproduce.py` creates all event rows itself: treatment 1/1 conversion; control 190/1000. It invokes public `get_ab_test` with the binary default in each source.

| Result | Frozen baseline220 | Candidate |
|---|---:|---:|
| p-value | .039364249056830426 | .19080919080919082 |
| Verdict | increase | inconclusive |
| Method | pooled z | Fisher exact |
| Delta CI | [.0161401949,.8331107299] | [−.2069429525,.8371088336] |
| Candidate labels | — | `fisher_exact_two_sided`, `clopper_pearson_bonferroni` |

The independent fixed-table Fisher reference equals the candidate. The baseline z statistic matches direct pooled-binomial algebra: the defect is its inferential validity on this accepted small-arm design, not a transcription error. The candidate retains the point estimate and identifies the conservative interval and nonduality. Relative confidence bounds are unavailable for the exact default rather than implicitly labeled exact.

Evidence: `baseline/public_small_arm.json`, `candidate/public_small_arm.json`, and `small_arm_reference.json` in each directory. All source imports are checked to come from the selected tree and recorded in `provenance.json`.

## Exact confirmation that multiplicity does not repair invalid p-values

A separate exact null enumeration derives Bernoulli sample variances and Welch–Satterthwaite degrees of freedom from counts for n=2 and n=100, common success probability .085. Only Student-t and binomial probability infrastructure comes from SciPy. It enumerates 303 pairs of outcome counts, with unavailable outcomes contributing no rejection.

| Decision rule | Unconditional null rejection probability |
|---|---:|
| Sparse binary Welch, alpha=.05 | .8232391255251323 |
| Holm or Bonferroni over ten identical declared outcomes | .5288866793458029 |
| Fisher, alpha=.05 | .0074468032 |
| Unavailable Welch probability | .0001171341 |

Ten identical named outcomes form a legitimate conservative family for this counterexample: Holm and Bonferroni multiply their equal p-values by ten. Despite that adjustment, rejection remains over 52% under the all-null model. The required valid-marginal-p-value premise is missing. The construction is not a claim that real experiments always contain duplicate metrics, or that every correction has this behavior. The scalar null probability closely reproduces role 01's independently enumerated value.

Evidence: `sparse_binary_exact_enumeration.csv` and `sparse_binary_exact_summary.json`. This is deterministic probability enumeration, so there is no Monte Carlo interval.

## Fresh regular-versus-skew simulation

The generator uses new seed **4402026**, 400 independent experiments per condition, 499 multinomial whole-unit bootstrap draws per experiment, and separate resampling seeds starting at 4408000 (regular) and 4418000 (skew). Both arms have the same null distribution. The script invokes the exported resampling engine, then independently computes mean contrasts, percentile endpoints and normal-SE p-values. Three datasets in each condition additionally traverse the public scorecard with explicit `test="bootstrap"`; their event CSVs and outputs are saved.

| Population | n control / treatment | Coverage count / trials | Coverage; 95% Wilson MC interval | Rejections / trials | Rejection; 95% Wilson MC interval |
|---|---:|---:|---|---:|---|
| Normal(10,1) | 40 / 60 | 372/400 | .930 [.90069,.95113] | 29/400 | .0725 [.05095,.10218] |
| Lognormal(0,2) | 15 / 150 | 294/400 | .735 [.68966,.77587] | 78/400 | .195 [.15915,.23665] |

All 800 intervals are available in this generator. Monte Carlo SEs are .012757/.012966 for regular coverage/rejection and .022067/.019810 for skew coverage/rejection. The candidate's explicit approximation results equal the frozen baseline for these samples. The numerical resampling fixes do not turn the underlying approximation into a universal finite-sample method.

This reproduces role 03's qualitative contrast: the skew/unbalanced design performs much worse than the regular design. The new regular run is itself somewhat liberal; its rejection interval narrowly excludes .05. **It is not evidence of exact nominal calibration**, and the seed was not changed or the simulation rerun to obtain more favorable numbers. Four hundred trials are modest, sufficient for the large regular-versus-skew difference and inadequate for fine calibration claims. Finite lognormal variance does not make a small unbalanced experiment representative of its population tail. More bootstrap draws cannot add missing population tail information.

The public candidate bridge also confirms nonbinary `auto` produces descriptive output, while explicit bootstrap retains approximate method labeling. This is the policy distinction under review, not removal of arithmetic or a proof that every nonbinary dataset is unsafe.

## MDE and allocation cross-checks

The candidate's root/role-01 changes reject planning power below .5 and label finite MDE as `normal_approximation`. The new role-04 tests confirm the small-n Welch numeric approximation is unchanged and its reading requires verification of actual chosen-test power. This resolves the negative accepted-power invariant and restricts the claim; it does not implement an exact power calculator. A binary normal approximation can still exceed the feasible effect range in a tiny arm (the small-arm fixture reports 110.14 percentage points), so it must remain planning information rather than an attained-power promise.

The role-01 positive-support SRM primitive patch was independently checked: `srm_chi2([68,32,0],[.5,.5,0])` returns Q=12.96, df=1, p=.00031821718031506765, matching `erfc(3.6/sqrt(2))`. An observed count in a zero-probability category returns p=0. Evidence: `primitive_zero_share_cross.json`. Role 04 did not edit the shared primitive.

## Reproduction and provenance

```sh
PYTHONPATH=.work/ab-safe-bet/baseline220/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/04_multiplicity/cross_review/cross_reproduce.py --source .work/ab-safe-bet/baseline220 --output output/AB_Safe_Bet_Review/roles/04_multiplicity/cross_review/baseline --trials 400
PYTHONPATH=.work/ab-safe-bet/repo/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/04_multiplicity/cross_review/cross_reproduce.py --source .work/ab-safe-bet/repo --output output/AB_Safe_Bet_Review/roles/04_multiplicity/cross_review/candidate --trials 400
```

The candidate is an actively edited integration checkout, not a finalized release commit. Each run records module paths/hashes and versions; final built-wheel/full-suite/CI verification belongs to the coordinating release review. This report makes no claim to have run those checks. No initial evidence was overwritten, and this role made no commit, push or subagent delegation.
