# Population, randomization unit, windows and strata audit

**Reviewer role 2/10 — causal inference / experimentation scientist. Date: 2026-10-30.**

**Release decision:** retain the strict fixed-calendar `experiment_data` population contract and the tested scorecard roster/window behavior. **Block automatic certification of pre-exposure strata from a path truncation.** Require an explicit analyst declaration or validated assignment-relative timing. The current heuristic can assign confirmatory treatment verdicts to a post-treatment collider.

## Provenance and independence

- Candidate audited: PR220, commit `38cbe91d5b315584575b132a596890e69eb210aa`, immutable checkout `.work/ab-safe-bet/baseline220`.
- Comparison: PR217, commit `7e61d997bcbd9bae9f1571ba3521a85fcb59f751`, checkout `.work/ab-safe-bet/pr217`.
- `PRE-REGISTRATION.md` and the standalone `oracle.py` were written and SHA256-frozen before any implementation/source inspection. Only the review protocol and role assignment had been read. The oracle has not been edited since freezing. SHA256: `37e80e38bb274d0329d4edb62825721fe4ce2082d20ca239b5eac0e06f5ca872`.
- After freezing, public docstrings, adapter/source implementation, repository instructions and selected existing tests were inspected. Independent test inputs were hand constructed or generated from seed `2026103002`; they were not copied from repository tests. No other reviewer reports were read. No source files were edited.
- Runtime: Python 3.13.17; pandas 3.1.0; NumPy 2.5.4; macOS 15.7.3 ARM64. Imported source paths and implementation SHA256 values are recorded in `baseline220/raw-results.json`.
- This is staged oracle-first review, not a formally blinded human study or a production causal-identification certificate.

## Executed evidence

**347 core and supplemental checks: 346 passed, 1 causal-safety failure.** The one failure is reproduced through three independent public transformation routes. Separate historical probes compare four contracts across PR217 and PR220.

- 38 hand-constructed core checks: 37 passed and the strata-provenance defect failed.
- 60 deterministic randomized datasets × 5 modes = 300 passing aggregate checks. Each dataset has 11–32 assigned units and 40–115 event records; 4,620 events in total. Modes: scorecard aggregate, fixed horizon, calendar bins, since-assignment bins and strict fixed-calendar analysis. Each comparison checks both arms' population counts, binary conversions and event-count means against the frozen oracle. Boundary/prehistory events and silent assignees are included.
- 9 additional population/strata/session checks passed, including dynamic-arm session overlap and explicitly descriptive active populations.
- These are deterministic randomized property checks, not a Monte Carlo study of false-positive rates or interval coverage. They supply no coverage or type-I-error guarantee; Monte Carlo uncertainty for estimator calibration is not applicable to this test count.

### Passing contracts and their limits

| Scope | Observed evidence | Release assessment |
|---|---|---|
| Explicit roster, silent assignments | Both APIs preserve assigned units with zero supported outcome; an entirely silent outcome window also works | Retain for supported binary/additive outcomes |
| Prehistory and arm consistency | Pre-assignment events are excluded; prehistory arm changes allowed; post-assignment contradictions rejected | Retain; authoritative roster still depends on correct user-supplied assignment data |
| Exclusive boundaries | Assignment/start events included, cutoff and horizon endpoints excluded | Retain tested half-open windows |
| Horizon maturity | Units with assignment + horizon ≤ cutoff retained; immature units excluded; explicit cutoff required | Retain independent-unit analysis, with logging completeness an analyst assertion |
| Calendar and relative bins | Hand and randomized counts/means match; quiet bins preserved with declared cutoff; staggered populations handled as documented | Calendar bins compare all units assigned before bin end, with unequal exposure durations when entry is staggered; interpret the stated estimand |
| Unknown or explicitly post-exposure strata | Descriptive verdicts, exploratory flag, no confirmatory p-value; silent units survive in roster strata or missing stratum | Retain descriptive use |
| Explicitly asserted pre-exposure strata | Declaration retained and silent roster members included | Require a defensible declaration; API cannot validate causal truth of an arbitrary flag |
| Coarsest declared grain | Scorecard and strict API reject session inference under a user-primary schema, including a session-indexed assignment table | Retain conservative guard; schema order is not proof of actual independence |
| Nested observed sessions | Shared users across dynamic arms counted in pooled cluster metadata; duplicate session keys spanning users rejected | Descriptive session contract only; no independent Bernoulli interval on sessions |
| Invalid joins and assignments | Duplicate/null assignment IDs rejected; strict API rejects unmatched event units; scorecard reports unassigned path count | Strict API is preferable for a confirmatory release surface; scorecard permits auditable cohort exclusion by contract |
| Event-only analysis | Output labels population as `stream` | Descriptive/observed-cohort scope; activity-selected records cannot enumerate all randomized assignees |

The session guard rejects ambiguous globally reused session identifiers. The independent oracle regards `(user, session)` as the natural nested key; callers can construct that composite identifier explicitly. Rejection is safer than silently joining distinct users' sessions.

## Finding P02-01 — high severity: post-treatment truncation certifies a baseline stratum

**Classification:** causal-safety/contract defect in automatic provenance. Statistical calculations on the selected records are internally consistent; the issue is the unearned pre-exposure claim and confirmatory verdict. The implementation's internal docstring acknowledges the heuristic cannot know treatment timing, but users do not make an explicit pre-exposure assertion in this reproduction.

**Public reproduction:**

```bash
.work/ab-integrated-wheel-env/bin/python \
  output/AB_Safe_Bet_Review/roles/02_population/reproduce_post_exposure.py \
  --source-root .work/ab-safe-bet/baseline220 \
  --output output/AB_Safe_Bet_Review/roles/02_population/baseline220/post-exposure-routes.json
```

The regression intentionally exits 1 on this baseline. Its construction sets the same outcome vector in each arm: `Y(i,A)=Y(i,B)=1[i<100]` for 200 units per arm, hence individual treatment effect zero and 50% conversion in each arm. A post-treatment engagement event is generated for A indices `<150` and B indices `>=50`, at hour 5. Purchases happen at hour 6. A cut marker occurs at hour 10, after assignment at hour 0.

The following public operation produces the unsupported declaration, without `pre_exposure=True` or `strata_pre_exposure=True` anywhere:

```python
cut = stream.truncate_paths('path_start', 'cut_after_treatment').add_segment(
    'post_engagement',
    metric_bins={
        'metric': {'metric': 'event_count',
                   'metric_args': {'event': 'engagement'}},
        'edges': [.5], 'segment_levels': ['no', 'yes'],
    },
)
cut.get_ab_test(('arm', 'A', 'B'), metrics,
               assignment=roster, strata_col='post_engagement',
               observation_end='2026-01-02 06:00:00', min_support=1)
```

**Expected:** subgroup comparisons remain descriptive: `strata_pre_exposure=False`, `exploratory=True`, no confirmatory p-value/verdict. Conditioning on this treatment-induced variable changes group composition even though the outcome has zero treatment effect.

**Actual, minimal single-metric reproduction:**

| Post-treatment stratum | A | B | Adjusted p-value | Verdict | Provenance |
|---|---:|---:|---:|---|---|
| No engagement | 0 / 50 | 50 / 50 | 3.05e-23 | decrease | `True`, `recipe`, `exploratory=False` |
| Engagement | 100 / 150 | 50 / 150 | 7.76e-9 | increase | `True`, `recipe`, `exploratory=False` |

No warning is emitted. The full two-metric audit has a larger multiplicity family and therefore different adjusted p-values; `baseline220/post-exposure-strata-actual.json` preserves those exact outputs.

**Propagation routes, all publicly reproduced:**

1. Direct: truncation → metric-derived segment → scorecard.
2. Export/import: `cut.get_path_segments(...)` emits `attrs['pre_exposure']=True`; `stream.add_segment(..., path_values=labels)` preserves it.
3. Replay: `Eventstream.from_recipe(frame, transferred.recipe())` preserves the serialized claim.

**Source locations in PR220:**

- `src/retentioneering/tools/ab_test.py:841–857`: explicit declarations are checked, then any prior truncation ending at an event makes a behavior-derived segment pre-exposure. `844–847` does not compare cut/event times with `assigned_at`.
- `src/retentioneering/tools/ab_test.py:877–878`: recipe `True` is accepted as a pre-exposure flag.
- `src/retentioneering/tools/ab_test.py:2280–2286`: that flag determines whether the result is descriptive and its p-value suppressed.
- `src/retentioneering/eventstream/eventstream.py:464–466`: inferred flag exported to path-label Series attributes.
- `src/retentioneering/ops.py:161–163`: Series flag serialized as `_path_values_pre_exposure`.
- `src/retentioneering/ops.py:197–200`: serialized flag restored on recipe replay.

**Remedy/release restriction:** do not infer a true pre-exposure flag from arbitrary truncation. Keep such segments descriptive unless the caller explicitly attests to timing or a future machine-checkable contract validates every stratum input before assignment/exposure. Preserve explicit declarations as declarations, and preserve `False`. A timestamp check must cover all data used to create the stratum; checking only that some truncation exists is insufficient. Invalidate or distinguish legacy inferred Series/recipe flags so an old automatic `True` is not laundered into a new confirmatory call.

**Regression:** `reproduce_post_exposure.py` must pass (exit 0) through all three routes. Add a positive-control regression that a deliberately supplied, documented `pre_exposure=True` still works, and a negative control that unknown/post-exposure source columns remain descriptive. The frozen aggregate oracle does not need to change.

## Historical comparison with PR217

`compare_population_contracts.py` runs the same public probes in isolated imports; raw outputs are in `pr217/comparison.json` and `baseline220/comparison.json`.

| Probe | PR217 | PR220 |
|---|---|---|
| Horizon without explicit logging cutoff | Returns inference using latest event as observation end | Rejects missing `observation_end` |
| Six sessions per arm from two users per arm, session-index roster | Accepts sessions as independent paths despite user-primary schema | Rejects finer-than-primary inference |
| Fully observed 36-hour logging period but last event at hour 1; 6-hour horizon | Cannot express cutoff and rejects all units as immature | Correctly retains both users per arm |
| Same quiet tail; 6-hour relative bins | Cannot express cutoff; no mature bin returned | Returns all six bins, including quiet bins |

These are resolved historical limitations and must not be reintroduced when selecting a smaller release surface. PR217 by itself is not a safe substitute for the PR220 population contract.

## Proposed safe release boundary

1. **Retain:** strict fixed-calendar `experiment_data` with a complete one-row-per-unit roster, explicit observation start/end and completeness, supported binary/additive outcomes, and the coarsest declared independent unit. Maintain missing/conflicting assignment validation.
2. **Retain with explicit constraints:** scorecard horizons and since-assignment bins require a logging cutoff; population and maturity diagnostics must remain visible. Confirmatory analyses should supply a complete authoritative roster rather than rely on observed activity. A fixed-calendar bin's staggered entry is a variable-exposure estimand.
3. **Restrict:** automatically inferred pre-exposure strata; unknown and behavior-derived strata remain descriptive. Conditional/active populations and session funnels remain descriptive even if they have uncertainty estimates.
4. **Defer:** session/user cluster-randomized causal claims, paired designs, interference, informative censoring, logging loss correction and post-treatment subgroup causal effects. Merely naming a finer unit as primary does not prove independence.
5. **Do not claim:** external validity, randomized assignment, completeness of logging, calibration under every event distribution, or all inputs safe from these finite tests.

## Reproduction and artifacts

The complete main audit supports an explicit source path and output directory, leaving the oracle unchanged:

```bash
.work/ab-integrated-wheel-env/bin/python \
  output/AB_Safe_Bet_Review/roles/02_population/audit.py \
  --source-root .work/ab-safe-bet/baseline220 \
  --output-dir output/AB_Safe_Bet_Review/roles/02_population/baseline220 \
  --random-n 60
```

Use the same command with a candidate checkout and a new output directory for a fix rerun. Run `supplemental.py`, `compare_population_contracts.py`, and `reproduce_post_exposure.py` with the same source root as additional regressions.

Artifacts: `PRE-REGISTRATION.md`, `PRE-INSPECTION-SHA256.txt`, frozen `oracle.py`, public `audit.py`, `supplemental.py`, `compare_population_contracts.py`, minimal `reproduce_post_exposure.py`, baseline raw JSON, exact generated fixtures, source hashes, command log and this report in Markdown/HTML. `REPORT.html` includes every core and supplemental check with expected/actual values.

### Protocol deviations and corrected harness assumptions

The oracle and preregistration were not modified. Adapters were necessarily written after signature inspection. The first smoke used `preprocess=False` for a path-truncation fixture, causing a missing event-index error; the fixture was changed to normal public preprocessing before the substantive run. An initial session validation probe incorrectly called a nonexistent Eventstream convenience method, and its broad exception recorder counted `AttributeError` as rejection. The authoritative run calls the exported public `tools.experiment.session_funnel_data` function and permits only intended validation exception types. All 347 reported checks use this corrected recorder. Initial logs/results are preserved as superseded exploratory harness evidence; only `baseline220/` is the authoritative baseline result directory.
