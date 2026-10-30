# ADR-0018: Integrate experiment analysis around shared statistical contracts

Status: Proposed for the integration branch (2026-10-30). Supersedes the conflicting
API defaults, unit guards, observation-window and bootstrap availability decisions
of [ADR-0017](0017-ab-testing-tools.md). Neither source PR is merged into master by
this decision.

## Context and source revisions

Two independently developed, individually tested branches add overlapping experiment
analysis to the same base, `e597bfbdb63ad5a5fb517894f4f68eda9ca84e36`:

- [PR #216](https://github.com/retentioneering/retentioneering-tools/pull/216), reviewed
  at `52615dd93476ef8532487792f26d023e944dfe77`, adds a strict assignment-roster report,
  explicit inference, safe handling of silent units, ordered experiment funnels,
  Lumeo fixtures, two tutorials and empirical method validation.
- [PR #217](https://github.com/retentioneering/retentioneering-tools/pull/217), reviewed
  initially at `5bef2877117e4936aed31986a69aa81ce4d6fefc` and refreshed to
  `a8182cffc223c5b27917916860ed636b88a458a8`, adds a comparative scorecard, a metric
  contribution matrix, calendar and exposure-age periods, follow-up horizons,
  stratification, whole-path bootstrap, SRM, MCP, interactive widgets and six
  synthetic teaching cases.

Passing each branch's tests independently does not establish that their population,
randomization unit, missing-data rules or statistical defaults agree. Merging both
implementations without reconciling those contracts would expose incompatible
answers behind similar method names. The integration starts from #217 and retains
#216 as a merge parent, preserving both histories and the source PR links.

## Decision

The final #217 increment (`a8182cf`) proposed suppressing repeated ID-match
warnings in a process-global set keyed by message text. That change is deliberately
not adopted: two different assignment tables with identical counts can share a
message, and a later MCP call would then omit a relevant diagnostic from its own
result. Repeated-call diagnostics remain self-contained. The source revision is
preserved in the merge ancestry; this exclusion is an explicit review decision.

### 1. Two explicit adapters, one numerical kernel

The primary flexible entry point is `Eventstream.ab_test_data`, with `ab_test` as
its interactive twin. `get_ab_test` remains a compatible spelling that reaches the
same engine. `get_ab_paths`, `get_ab_bootstrap` and `get_sample_ratio` retain their
specialized purposes; they are not alternate implementations of the scorecard.

`experiment_data` / `experiment` remains the strict fixed-calendar adapter. It is
useful for a frozen, aggregate JSON report with an authoritative roster, an
explicit complete-observation declaration and deliberately selected inference
methods. It is **not** an alias of `ab_test_data`: the following differences are
intentional and observable.

| Contract | `ab_test_data` / `get_ab_test` | `experiment_data` |
|---|---|---|
| Output | Tidy DataFrame with audit columns | Versioned aggregate JSON snapshot |
| Population source | Assignment table recommended; event-observed comparison permitted and labelled | Assignment roster required |
| Assignment shape | Index identifies units; `arm`, optional `assigned_at` | Unit ID and named variant/time columns |
| Window | Observed span, explicit logging cutoff, fixed follow-up, calendar or exposure-age bins | Fixed `[start, end)`, all assignments at or before start; `complete=True` required |
| Effect direction | `diff=(column, group1, group2)` means group1 minus group2 | Treatment minus control |
| Metrics | Registry metrics, named outcomes, optional ratio-of-sums denominator | Explicit mean, ratio of sums, or eligible mean of individual ratios |
| Default inference | Binary z/Wilson/Newcombe; Welch for totals/values; whole-unit bootstrap for ratios | No implicit inference; request bootstrap, binary intervals and/or explicit tests |
| Tests | Auto dispatch or selected method; bootstrap SE approximation explicitly labelled | Explicit Fisher exact / Welch mapping; intervals and hypothesis tests configured separately |
| Multiplicity | Holm across all declared rows in a call by default | Holm across explicitly requested hypotheses |
| Stratification/time comparison | Pre-exposure strata, periods, joint early/late contrasts | Deliberately outside this adapter |
| MCP | Session-bound roster and scorecard/contrast tools | Python/report surface; no second MCP testing engine |

This is a staged integration, not a claim that two public vocabularies are ideal
forever. Collapsing them now would either weaken the strict population contract or
break the richer period/strata workflow. A future deprecation needs an explicit
conversion specification and migration period. No existing master API is silently
redefined into an experiment roster API.

Shared implementation boundaries:

- `stats/primitives.py`: Wilson/Newcombe intervals, stable sample summaries and
  Welch calculations, correction procedures and other shared inference formulas.
- `stats/resampling.py`: whole-unit multinomial/Poisson count weights, weighted
  sums, and percentile-interval availability. Numerator and denominator, periods
  and metrics for one unit always receive the same weight.
- `stats/experiments.py`: strict outcome adapter using those primitives;
  `tools/_stats.py` is a thin compatibility import surface.
- `tools/_bootstrap.py`: flexible matrix orchestration over shared weights and
  interval rules. Matrix layout and experiment window selection belong to the
  adapter; neither implements a second numerical bootstrap kernel.
- `paths/ordered_reach.py`: common ordered-step SQL, with aggregate output for
  ordinary funnels and unit contributions for experiment uncertainty.
- `metrics/metric_builder.py`: the single existing metric registry. New totals
  compose with path metrics, segments and visualizations rather than defining an
  independent A/B-only event-processing language.

### 2. Population and observation time are data contracts

An assignment roster includes every assigned unit, including zero-event users.
An event log cannot reveal users with no events. Without a roster, the scorecard
therefore labels its population as event-observed rather than claiming full ITT.
A deliberately all-silent cohort is accepted when its assignment index explicitly
names the declared analysis unit; an unnamed entirely unmatched table is rejected
because an accidental join-key error is otherwise indistinguishable from silence.

`observation_end` is the **exclusive timestamp through which logging is declared
complete**. It is required for `horizon` and `period_basis="since_assignment"`.
The last recorded event is never proof of complete observation. A unit matures when
its assignment time plus the requested horizon is at or before this cutoff; its
own activity does not determine maturity. Empty but fully observed trailing bins
remain present. Events at or after the cutoff do not contribute. Compared
assignments at/after the cutoff fail as an invalid window rather than becoming
observed zero outcomes.

Without an explicit cutoff, permitted overall/calendar comparisons report
`observation_basis="observed_event_span"`. With one they report
`"declared_logging_cutoff"`; `observation_end` and `analysis_unit` are exported.
This metadata does not prove logging completeness: the analyst supplies that
assertion. Missing telemetry is not a zero outcome.

Pre-assignment source history is allowed, and excluded from post-assignment
outcomes. Post-assignment event arm labels must agree with the roster. Null IDs,
invalid assignment timestamps, duplicate assignments and inconsistent membership
fail before inference. Half-open boundaries are tested explicitly.

### 3. Independence is declared, never inferred from sample size

The ordinary inferential funnel/conversion and flexible scorecard require the
coarsest declared path grain. Explicit ID splits, small samples and rare switches
do not bypass this requirement. Finer-grain point-only exploration is permitted.
The former heuristic based on at least ten multi-session users and a 99% constant
arm share left small samples and alternate diff forms unprotected.

The strict adapter's `unit_col` must also identify the coarsest declared grain.
For a genuine session-randomized design, sessions can be declared as the
primary independent unit in a suitable schema. Neither API can verify the actual
randomization mechanism. Cluster-randomized, paired and crossover designs require
appropriate clustering/pairing logic; declaring sessions independent does not
supply it. Session funnels in the strict report resample their independent users
when that is the declared experiment unit.

SRM describes its independent-assignment sampling assumption. A small SRM p-value
is a reason to investigate assignment, logging and exclusions; it is not proof
that randomization is broken. Fixed-allocation designs need a compatible audit.

### 4. Ratios and missing values retain their estimands

A ratio of totals is `sum(numerator) / sum(denominator)`, not the mean of individual
ratios. Joint unit resampling preserves numerator-denominator dependence. Finite
nonnegative denominators are required; incomplete numerator/denominator pairs are
excluded together and reported as conditional. A zero denominator is not silently
converted into an individual zero ratio.

The strict adapter separately supports mean-of-ratios with explicit positive-
denominator eligibility. That estimand stays explicit; it is not substituted for
the scorecard's ratio of sums.

`event_value_sum(event=..., value_col=...)` is retained alongside `column_sum`:
the former rejects missing or non-finite values on matching events, while unrelated
events do not affect it. No matching event yields a total of zero. The broader
column metrics retain their own documented missing-value rules. They are not
interchangeable when an event has missing monetary telemetry.

### 5. Bootstrap failures stay visible

All requested replicates must be finite for an empirical interval or bootstrap
standard error to be available. Dropping undefined ratio draws conditions on a
positive denominator and changes the bootstrap distribution. One undefined draw
therefore makes the interval unavailable; the point estimate, counts, valid-draw
count and reason remain inspectable. Increasing replicates is not a remedy for
sparse support or heavy tails.

An original constant arm has no empirical arm interval. A contrast remains
estimable when the other arm varies; two constant arms do not justify a zero-width
empirical interval. Binary Wilson/Newcombe intervals remain an explicitly selected
alternative in the strict adapter and the analytic default in the flexible one.

The shared sampler is versioned as `whole_unit_counts_v1_baseline_first`: seed
child 0 draws the baseline/control/group2, child 1 the comparison/treatment/group1.
Matching unit contributions, row ordering, seed and scheme yield matching draws
across adapters. Finite Monte Carlo realizations change relative to #217's former
orientation; this is an intentional pre-release change. Distributional validity is
not a promise of identical streams across future algorithm versions. Strict session funnels use `whole_unit_counts_v1_pooled_union`
for their distinct pooled-user-cluster design rather than mislabelling those draws
as independent-arm sampling. The flexible
adapter requires at least 100 draws; the strict adapter permits smaller diagnostic
runs with a warning. Production precision and empirical coverage remain separate
questions.

### 6. Correct the declared family and label exploratory slices

The flexible default is `correction_scope="all"`: all declared hypotheses in the
returned call, including unavailable rows, contribute to family size. An unavailable
test must not make the remaining tests easier to reject. Explicit `"metric"`
keeps separately declared families possible. Widget tabs and MCP overall/strata/
period calls are separate families; no adjustment claims to cover arbitrary
interactive exploration, optional stopping or unreported analyses.

Per-comparison intervals may exclude zero while a multiplicity-adjusted verdict
is inconclusive. The reading and UI state that distinction. Bonferroni simultaneous
intervals are separately named. Statistical significance does not demonstrate a
useful effect; failure to reject a guardrail does not establish non-inferiority.

Unknown or post-exposure strata are descriptive: suppress confirmatory p-values,
verdicts, heterogeneity, balance and stratum-versus-rest tests. Keep descriptive
estimates and intervals with an explicit label. Inference requires a declaration
or recognized recipe showing membership fixed before exposure. Active-period
populations have the same descriptive restriction. MCP contrast summaries must
obey these rules as well as scorecard rows.

### 7. Funnel and MCP are adapters, not parallel statistical products

Ordinary `funnel_data` remains a visualization of observed paths; it does not
invent assigned silent users. Its payload says `population_basis="observed_paths"`
and declares the independence assumption for intervals. An experiment funnel uses
the authoritative roster, so its first-step denominator can differ legitimately.
Ordered-step extraction is shared. Ordinary aggregate funnels avoid materializing
a full unit-by-step matrix in Python solely to count steps.

Cumulative conversion uses the declared independent population. Conditional
step-to-step conversion selects users by a post-assignment event and is descriptive:
intervals can aid diagnosis, but confirmatory p-values are suppressed consistently
in conversion and funnel surfaces. An apparent checkout improvement can coexist
with worse overall assignment-level conversion.

MCP keeps `serve(stream, assignment=table)` as the roster binding. Cutoff and family
parameters pass through server, tools, widgets and Python APIs. JSON must remain
finite/JSON-safe and preserve population, cutoff, descriptive labels, inference
availability and correction scope. The wrapper must not calculate a second result
or discard inconvenient statuses. Source mutation tools are not involved.

## Regression and teaching plan

The integration includes targeted regression suites, alongside the original tests:

| Risk | Independent evidence |
|---|---|
| Two numerical engines drift | Identity checks for shared functions; matched bootstrap draws; public adapter parity for means, ratios and binary intervals |
| Silent users disappear | Roster-only users, entirely silent named cohorts, manual group denominators and MCP parity |
| Last-event maturity bias | Explicit cutoff, quiet tails, zero-event complete bins, boundary events and future-assignment rejection |
| Ratios change meaning | Joint numerator/denominator draws, incomplete pairs, negative denominator rejection, undefined-draw failure |
| Repeated sessions appear independent | Small-sample and explicit-ID grain guards; point-only finer-grain path |
| Post-exposure slices get confirmatory labels | Strata and active-period suppression through scorecard and contrast summary |
| Unavailable outcomes shrink corrections | Missing hypotheses retained in Holm/Bonferroni/BH families |
| Numerical magnitudes break inference | Tiny/huge scales, decimal constancy and one-constant-arm regression cases |
| Funnel drift or performance regression | Shared reach semantics, source event catalog, conditional labels and aggregate SQL projection |
| MCP drops a contract parameter | Signature/schema sync; cutoff propagation; direct-Python/result parity and strict JSON serialization |
| Teaching prose becomes stale | Four executable notebooks with narrative assertions; frozen fixture integrity |
| Rendering differs from data | JavaScript tests, built widget bundles and browser rendering tests |

The Northwind walkthrough shows how scorecards combine with segment overview,
funnels, exposure-age contrasts and pre-exposure clustering. The showcase adds
A/A calibration, telemetry drift, billing loops, trial conversion, Simpson-like
composition and selection pitfalls. The strict and Lumeo tutorials demonstrate
assignment denominators, session diagnostics, explicit methods and frozen reports.
All examples are synthetic or documented synthetic teaching fixtures, not evidence
of production treatment effects.

Historical coverage audits are retained with their original hashes and conclusions.
New audit provenance includes hashes of the shared primitive and resampling modules;
hashing the former single adapter file alone would no longer identify the method.
Heavy-tailed monetary examples previously exhibited substantial undercoverage and
inflated error rates. The shared kernel fixes numerical and contract defects; it
does not make percentile intervals or Welch universally calibrated for such data.

## Consequences and limits

The integration is larger than a textual conflict resolution because tests and
examples encoded incompatible assumptions. Defaults and formerly accepted calls
change before release: missing horizon cutoffs now fail, finer-grain inference is
stricter, sparse bootstrap intervals can be unavailable, unknown strata lose
confirmatory labels, and the default multiplicity family is broader. CHANGELOG and
tutorials describe these changes; source PRs remain linked for review.

The scope excludes sequential testing, sample-ratio procedures for every allocation
design, CUPED, automated causal identification, general cluster bootstrap and a
unified high-level analysis-plan object. Those need separate estimand and design
specifications. This branch is a reviewable integration proposal, not authorization
to merge either source PR or this branch into master.
