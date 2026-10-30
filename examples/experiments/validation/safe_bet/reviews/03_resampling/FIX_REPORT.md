# Bounded resampling fixes — candidate implementation

Completed 2026-10-30 in `.work/ab-safe-bet/repo`; no commit, push or subagents. Initial oracle, reports, baseline inputs, Monte Carlo evidence and their revision-prefixed outputs remain unchanged. This phase is explicitly unblinded implementation and cross-review. No large simulation rerun was needed for these deterministic restrictions.

## Implemented restrictions

1. **Numeric overflow is an explicit error.** Shared checked sums, positive-denominator ratios and effect differences raise `ValueError` containing `Numeric overflow` when arithmetic exceeds the supported range. Raw bootstrap checks both original aggregate sums and every weighted batch. This covers descriptive scorecard points without resampling, explicit advanced bootstrap scorecards, strict reports with or without requested bootstrap, and the raw draw kernel. Legitimately undefined denominators still produce unavailable draws/statuses rather than observed zeros. The policy deliberately rejects unsupported intermediate range even when a mathematically rearranged result could be representable; it does not promise full IEEE-754-domain support.

2. **Paired-period eligibility is checked separately from zero denominators.** `MetricMatrix` retains finite-measurement eligibility for value components. Before constructing a contrast, the adapter compares these masks over the common roster across every contrasted period. Changing measurement eligibility raises `InvalidParameterError` with `common outcome eligibility`. Complete paired vectors and a fixed eligible subset retain joint weights. A finite, observed zero denominator is still a valid ratio contribution: its change across periods does not itself trigger this rejection. Structurally zero total/count contributions also remain eligible.

3. **Bootstrap resources are bounded before allocation.** Shared limits are 1,000,000 requested replicates and 10,000,000 replicate-by-contribution values. Numerator and denominator columns count separately. The matrix adapter checks the full request before splitting into chunks; the shared sampler checks its own allocation and bounds its temporary unit-weight batches. More than 10,000,000 weights in a single replicate is rejected. `n_bootstrap='auto'` fails clearly when the requested alpha/family implies an unsupported B. Descriptive defaults do not resolve or allocate unused automatic draws. These are implementation resource limits, not statistical recommendations or a bound on all DataFrame memory overhead.

4. **Unresolved Bonferroni percentile tails are unavailable.** Explicit bootstrap keeps the percentile adjusted interval construction. The adjusted endpoints are withheld unless `B * (alpha / declared_family_size) / 2 >= 10`, allowing a small rounding tolerance at equality. This matches the precision target implicit in the existing automatic-B formula. It is a diagnostic resolution threshold, not a coverage guarantee. A run with B=1000 and 100 hypotheses has only 0.25 expected replicates in each adjusted tail and cannot report finite adjusted endpoints under this policy. Ordinary pointwise intervals, exact binary intervals and other method calculations retain their existing rules.

## Payload/doc integration needed

Bonferroni rows now carry:

| Field | Values |
|---|---|
| `delta_ci_adjusted_basis` | `path_bootstrap_percentile_approximate` for bootstrap; otherwise the row's existing `ci_basis` |
| `delta_ci_adjusted_status` | `insufficient_tail_resamples`, `ok`, or `unavailable` |

When the tail is unresolved, `delta_ci_low_adj` and `delta_ci_high_adj` are NaN/JSON null. This metadata must survive widgets/MCP/exports; role08 and the coordinator were notified. Docs should state the numeric rejection, common-eligibility restriction, resource limits and adjusted-tail threshold. Eventstream/docstrings/docs/JS were deliberately not edited by this role. No switch to normal-SE simultaneous intervals was made.

The r01 exact-binary/default-descriptive dispatch, the coordinator's provenance/tail/MDE logic, sample-ratio surface, reading goldens, conversion/funnel code and `_strata_columns` were not changed by this role. The coordinator separately identified an asymptotic secondary-strata diagnostic leak and is taking ownership after this handoff; the passing checks here are not certification of that untested surface.

## Validation and audit trail

- New dedicated suite: `tests/tools/ab_safe_resampling_test.py`, 19 cases. On immutable baseline220, **17 fail and 2 positive controls pass** (`final_regressions_baseline.log`); the two controls are complete pairing and legitimate zero-denominator preservation.
- Final candidate focused run: **287 passed, 3 skipped**, with one expected pre-exposure provenance warning (`fix_final_focused.log`). Included the new regressions, existing bootstrap, numerical strict-report, binary-default, scorecard, statistical-integration, experiment and experiment-revision suites.
- Frozen-oracle public fixtures rerun under `--label candidate_fixed`. Ordinary means/ratios, joint missingness, scaling through 1e200 and complete pairing remain correct. The old overflow and changing-eligibility probes now intentionally error. `candidate_fixed_fixtures_console.log` records these phase errors explicitly; they are expected hardening outcomes, not concealed successful simulations. Dedicated regressions assert the error contracts.
- Independent cross-review: **12/12 checks pass**, including an integer-combinatorial Fisher reference, direct binomial-tail bisection for the exact difference CI, frozen-oracle descriptive means/ratios, no implicit draw allocation, zero-share SRM and declared missing-family Holm (`candidate_cross_review_v2/cross_review.json`).
- `ruff --select E9,F63,F7,F82` passes for all owned files (`fix_ruff.log`); formatting applied to owned changes. `git diff --check` passes. This is not a full-repository pre-commit claim.

The first candidate test run recorded 14 failures, including one audit-fixture error from calling `to_dataframe()` on an unprocessed frame without `exclude_start_end=False`. The fixture was corrected. The first post-fix paired tests expected `ValueError`, but the API correctly raises its existing `InvalidParameterError`; the tests now assert that class. These adapter corrections are disclosed rather than attributed to implementation failures. The final baseline run uses the corrected final regression suite.

Modified implementation files: `stats/resampling.py`, numeric portions of `stats/experiments.py`, `tools/_bootstrap.py`, and numeric/contrast/resource/adjusted-bootstrap portions of `tools/ab_test.py`. Existing peer edits in these shared files remain present. `FIX_SOURCE_MANIFEST.json` records the exact post-fix source/test hashes and unchanged frozen oracle. No unrequested source files were changed.

## Reproduction

From the candidate directory, with `PYTHONPATH=src` and the workspace's `.work/ab-integrated-wheel-env/bin/python`:

```sh
python -m pytest tests/tools/ab_safe_resampling_test.py tests/tools/bootstrap_test.py tests/tools/experiment_numeric_test.py tests/tools/ab_safe_binary_test.py tests/tools/ab_test_test.py tests/tools/statistical_integration_test.py tests/tools/experiment_test.py tests/tools/experiment_revision_test.py -q
```

From the workspace root:

```sh
.work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/03_resampling/cross_review_fixed.py --source .work/ab-safe-bet/repo --output NEW_CROSS_REVIEW_DIRECTORY
PYTHONPATH=.work/ab-safe-bet/repo/src .work/ab-integrated-wheel-env/bin/python output/AB_Safe_Bet_Review/roles/03_resampling/run_review.py --label NEW_LABEL --mode fixtures
```

Full repository tests, final wheel-only checks, browser/MCP metadata propagation, documentation reconciliation and CI remain coordinator release gates. This role has not declared the entire release ready.
