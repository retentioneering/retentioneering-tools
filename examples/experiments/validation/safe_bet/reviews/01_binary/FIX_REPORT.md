# Binary safe-surface implementation

Candidate `.work/ab-safe-bet/repo`, uncommitted changes on 2026-10-30. No commit, push, merge, package publication, PR221 extraction or source edits outside assigned ownership were performed. Existing provenance, tail, power, and MDE wording changes were preserved.

## Implemented policy

- `test='auto'` resolves known binary outcomes to `fisher_exact` and nonbinary outcomes to `descriptive`.
- Binary defaults use shared `fisher_exact_p`, pointwise Clopper–Pearson arm intervals and `exact_difference_ci`, the conservative Bonferroni combination of CP bounds. Relative effect points remain available when defined; **relative intervals are suppressed** on this exact path, so no unlabeled approximate ratio interval sits beneath an exact label.
- Explicit `z_test` retains Wilson/Newcombe intervals but requires at least five pooled-null expected successes and failures in each arm. Failure yields missing p and `insufficient_data`, with an explanatory support warning. This criterion is a restriction, not an exact calibration guarantee.
- Known-binary `welch_t_test` and scorecard `bootstrap` are rejected. Strict `experiment_data` rejects known-binary Welch as well. Explicit raw `get_ab_bootstrap` continues to generate draws for valid metric specs.
- Nonbinary descriptive defaults calculate the same point estimates, allocate no resampling, and return no p, CI or MDE. Explicit nonbinary Welch/bootstrap remain advanced approximate methods, identified in metadata and prose.
- Strict `binary_intervals=None` preserves the no-request contract. `{'method':'exact'}` selects conservative exact intervals; explicit `{'method':'newcombe'}` remains approximate.
- `srm_chi2` now drops zero-probability/zero-observed categories from df and flags positive counts in zero-probability categories, as requested by role 04.

## Integration contract for coordinator/UI/docs

Allowed scorecard test values: `auto`, `fisher_exact`, `z_test`, `welch_t_test`, `bootstrap`, `descriptive`. Metric-kind restrictions are enforced by `ABMetricSpec`.

| Selected method | ci_basis | p_basis |
|---|---|---|
| Fisher | clopper_pearson_bonferroni | fisher_exact_two_sided |
| Explicit z | newcombe_approximate | pooled_z_asymptotic |
| Explicit Welch | welch_approximate | welch_asymptotic |
| Explicit bootstrap | path_bootstrap_approximate | bootstrap_se_asymptotic |
| Descriptive | not_requested | not_requested |

New support warning: `insufficient_expected_cells`. A scorecard MDE that is available has `mde_basis='normal_approximation'`; otherwise `mde_basis='unavailable'`. This is not test-specific power, including on Fisher rows. The new column follows `mde_alpha`.

Strict interval method output names are `clopper_pearson` and `clopper_pearson_bonferroni`, or `wilson_approximate` and `newcombe_approximate`. The public selector remains `exact` or `newcombe`. The compatibility namespace `tools._stats` automatically exposes new shared primitives through its existing forwarding implementation.

Conversion/funnel default routing, Eventstream public docstrings, JS selectors/types and documentation remain coordinator/other-reviewer integration work. Their source files were not edited here. The one requested public parity test now compares exact scorecard intervals with strict `method='exact'`. Default exact intervals may be materially wider; docs must not claim agreement between CI exclusion and the Fisher verdict. Existing asymptotic heterogeneity/rest diagnostics remain separate advanced diagnostics and are not certified as exact by the Fisher label.

## Validation

- **Failing before:** all 11 final targeted regressions fail on immutable baseline220 (`baseline_regressions.log`). The earliest nine-failure run on the coordinator's partial candidate is separately preserved (`binary_before.log`).
- **Passing after:** final focused suites report **302 passed, 1 skipped**, with one expected provenance warning (`final_focused.log`). This includes binary policy, prior tail/power/provenance, old scorecard/strict adapter numerical tests and statistical primitives. A separate 57-test run covers public exact parity and the explicit advanced signed-ratio case (`final_targeted.log`).
- Frozen public reproducer now passes all four original defect expectations: finite highest-level Wilson bounds, no tiny-arm unsupported significance, no accepted-level crash, and no sparse binary Welch significance (`final_public.json`, `final_public.log`).
- **9,604 tables** compared to this role's frozen exact Fisher and Newcombe oracle; maximum differences 4.44e-16 and 3.33e-16. The new exact difference interval also agrees with a separately coded direct-binomial-tail bisection reference to 2.11e-15 (`fixed_exact/checks.json`). The supplemental reference imports no beta distribution routine and is explicitly post-cross-review, not retroactively preregistered.
- Conservative exact difference coverage enumerated at **2,527 parameter points**. Minimum observed coverage is 0.9876471773 at n=1/1000, pa=0,pb=0.5. There is no Monte Carlo uncertainty or seed. This finite grid is a regression check; the mathematical CP-tail and union-bound argument supports the coverage claim under the stated model (`fixed_exact/exact_coverage.csv`, `fixed_exact/summary.json`).
- Focused fatal/static checks pass (`ruff --select E9,F63,F7,F82`); formatting applied to owned files. Broad unrestricted Ruff reports existing style issues in legacy sections, so this is not a claim that full pre-commit is green.

Old tests expecting auto Welch/bootstrap now opt into those advanced methods explicitly where that is what they are testing. Tests for default dispatch assert the new contract. No old numeric expectation was changed to disguise an arithmetic mismatch. Alpha-rounding assertions now require preserving the actual positive floating tail. Approximate strict-interval labels were updated deliberately.

A broader integration run recorded 117 passes and 21 failures (`integration_initial.log`), largely old reading/SRM contracts. Those results were not hidden: role 04 owns the reading and SRM metadata updates; the coordinator owns Eventstream/MCP metadata integration. The signed-ratio test was fixed to opt into bootstrap and independently passed. Full repository tests, UI, docs, package wheel, notebooks, CI and release acceptance still belong to the coordinator's final gate; they are **not** asserted complete here.

## Reproduce without overwriting initial evidence

```sh
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/01_binary/verify_fixed.py --source .work/ab-safe-bet/repo --output NEW_DIRECTORY
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/01_binary/reproduce.py --source .work/ab-safe-bet/repo --output NEW_RESULT.json --expect-fixed
```

Use explicit source `PYTHONPATH` when invoking pytest in the candidate. The installed runtime distribution is not evidence of the edited source. `FINAL_SOURCE_MANIFEST.json` records final owned source/test hashes; `fixed_exact/summary.json` records the earlier statistical-check source hash, before the unrelated SRM df edit and final method metadata additions. The original oracle remains SHA256 `b87ea41de0a571dd70d59cb31b61c92f2ffaf2f6a0c63a5ba78c6b90241db227`.
