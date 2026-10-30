# Integration review: PRs #216 and #217

This record accompanies `codex/ab-integrated`. It describes integration-specific
findings and verification; it does not relabel the historical PR #215 coverage
audit as a new run. All examples are synthetic teaching material.

## Sources and architecture

- [PR #216](https://github.com/retentioneering/retentioneering-tools/pull/216),
  `52615dd93476ef8532487792f26d023e944dfe77`.
- [PR #217](https://github.com/retentioneering/retentioneering-tools/pull/217),
  initially `5bef2877117e4936aed31986a69aa81ce4d6fefc`, refreshed and reviewed at
  `a8182cffc223c5b27917916860ed636b88a458a8`.
- Shared master base: `e597bfbdb63ad5a5fb517894f4f68eda9ca84e36`.

The [integration ADR](../../../docs/adr/0018-ab-integration.md) is the detailed
architecture and contract comparison. It preserves two explicit adapters over
one statistical kernel: the comparative scorecard (`ab_test_data`, with
`get_ab_test` as a compatible name) and the strict fixed-calendar roster report
(`experiment_data`). Their methods are not interchangeable aliases: the roster,
window, estimand, default inference and output shape must be selected explicitly.

## Findings resolved in the integration

| Priority | Problem and consequence | Correction and evidence |
|---|---|---|
| P1 | Last-event-based maturity excludes silent but fully observed users and drops quiet trailing periods. | Explicit exclusive logging cutoff for horizons/exposure-age bins; zero-event complete-bin, cutoff-boundary and silent-roster regressions. |
| P1 | Nested session inference bypasses the guard in small samples, explicit ID splits or a session-indexed roster; repeated users inflate effective sample size. | Coarsest declared independent grain required across scorecard, SRM, strict report and funnel/conversion inference. Genuine session randomization needs a primary-session schema. |
| P1 | Dropping undefined bootstrap ratio draws conditions on denominator survival and can manufacture an interval. | All requested draws must be finite; unavailable interval/p-value and valid-draw count remain visible. |
| P1 | Constant decimal contributions or paired period changes can acquire rounding variance and produce `p=0`. | Original contribution constancy and operation-aware subtraction resolution checks precede contrast inference; one varying arm still supplies uncertainty. |
| P1 | Unknown/post-exposure strata or active-period contrast summaries can bypass descriptive status. | Suppress confirmatory scorecard/contrast p-values and subgroup diagnostics; retain descriptive estimates and labelled intervals. |
| P2 | Unavailable planned outcomes shrink multiplicity families; per-metric default encourages a multi-outcome winner selected after inspection. | Retain unavailable hypotheses in family size; default all declared rows per call; explicit per-metric families remain possible. |
| P2 | Ratio numerator and denominator missingness can use different units. | Joint pair eligibility, nonnegative finite denominators, ratio-of-sums estimand and whole-unit joint draws. |
| P2 | Missing event timestamps can silently remove compared orders from timed analysis. | Reject unknown time eligibility in compared outcomes rather than treating missing logging as zero. |
| P2 | Distinct typed labels such as integer `1` and string `'1'` collide in unused DuckDB categorical columns or exports. | Project metric SQL inputs, compare segment membership with typed values, preserve assignment label types. |
| P2 | Raw squared scales can overflow/underflow in Welch, bootstrap SE, Q or stratum-versus-rest diagnostics. | Shared normalized summaries, scaled heterogeneity and stable hypotenuse calculations, with extreme-scale regressions. |
| P2 | MCP and widget export can omit sampler identity, method basis, invalid-replicate status or observation cutoff. | Propagate audit metadata and unavailable inference through JSON, widget/static export and server wrappers. Binary contrast fields explicitly distinguish raw shares from percentage-point display. |
| P2 | Hashing only the strict adapter no longer identifies its imported statistical implementation. | Audit records actual dependency paths/hashes; replay from mismatched frozen adapter/dependency locations fails clearly. Historical standalone implementations still work. |
| P2 | A process-global warning cache keyed by message text can hide warnings for different rosters with identical counts and subsequent MCP calls. | The final #217 cache change is deliberately excluded; per-call warning results remain self-contained and are regression-tested. |

Ordinary funnels retain observed-path denominators, while experiment funnels use
the assignment roster. Their counts may differ legitimately. They share ordered
reach extraction, and ordinary aggregate funnels use SQL projection rather than
materializing a unit-by-step matrix merely to count steps. Conditional transitions
remain descriptive because reaching the previous step is post-assignment selection.

## Verification

The integration verification covers the full Python suite, repository lint and
hygiene, JavaScript tests and builds, browser rendering/sidebar behavior,
documentation generation, package build, four executed notebooks, optional slow
checks and an independent-method smoke simulation. Local source verification after the final contract fixes:

- Python 3.13.17: **2,915 passed, 9 skipped**; the optional slow cases are listed
  separately below. One existing joblib physical-core-detection warning remains.
- **50 JavaScript tests** and both frontend builds passed.
- **11 browser checks and 2 notebook smoke checks** passed in the combined run.
- All **8 optional slow cases** passed (7 in the sweep and the final session-grain
  case rerun after its fixture adaptation).
- Strict and Lumeo notebooks executed **13 and 17 code cells**, with zero errors.
- Documentation generation and repository pre-commit checks passed.
- The **15-scenario, 100-repetition, 999-resample** smoke completed with actual
  imported dependency hashes recorded. This is not a high-precision calibration.

The wheel-only verification and GitHub CI matrix are recorded in the pull request
against its published head. Source-only success is not substituted for either.

Useful reproduction commands from the repository root:

```bash
uv sync
npm ci --prefix js
npm run build --prefix js/viz-core
npm run build --prefix js/widget
npm test --prefix js/widget
uv run pytest tests --ignore=tests/browser --ignore=tests/notebooks -q
uv run pytest tests/datasets tests/tools -m slow --run-slow -q
uv run --with playwright python -m playwright install chromium
uv run --with playwright pytest tests/browser -q
uv run --with nbclient --with nbformat --with ipykernel pytest tests/notebooks -q
uv run --with nbclient --with nbformat --with nbconvert --with ipykernel python examples/experiments/execute_tutorial.py
uv run --with nbclient --with nbformat --with nbconvert --with ipykernel python examples/experiments/execute_tutorial.py --lumeo
uv run python docs/scripts/render_pages.py
uv run pre-commit run --all-files
uv build
```

For the small simulation, use the matching checkout on `PYTHONPATH`:

```bash
PYTHONPATH=src OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  uv run python examples/experiments/coverage_validation.py \
  --trials 100 --resamples 999 --workers 4 \
  --output artifacts/integration-coverage-smoke.json
```

A 100-repetition smoke checks execution/accounting, not nominal coverage to narrow
precision. The larger historical audit remains the evidence for severe-tail
limitations: percentile bootstrap and Welch can substantially under-cover or
inflate false positives for extreme monetary tails. More resamples do not fix a
poorly supported empirical distribution. The integration adds no sequential,
paired, cluster-randomized or censored inference guarantee.

## Teaching materials

- `notebooks/ab_testing_tutorial.ipynb`: Northwind scorecard, assignment population,
  funnel, period contrasts, pre-exposure segments and an explicit decision plan.
- `notebooks/ab_testing_showcase.ipynb`: six cases exposing SRM, composition,
  dependence, calibration, delayed outcomes, selection and missing telemetry.
- `notebooks/experiment_tutorial.ipynb`: strict roster report, explicit methods,
  behavioural patterns, session diagnostics and native offline widget.
- `notebooks/lumeo_experiment_tutorial.ipynb`: Lumeo's distinct estimands, assigned
  users versus sessions, sparse funnels, screen coverage and unavailable intervals.

The frozen Lumeo data tables and historical statistical audit JSON are unchanged.
Source PR branches remain intact. This integration branch is a review candidate;
it does not merge changes into master.
