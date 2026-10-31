# ADR-0017: A/B testing tools

Status: Accepted (2026-10). Supersedes the **StatTests** item of
[ADR-0012](0012-features-dropped-from-3x.md); the rest of ADR-0012 stands.

## Context

5.0 cut 3.x's `StatTests` (ADR-0012). What analysts asked for since is not a
two-sample test over a column, but an experiment readout: did the treatment
move a few path metrics, by how much, with what uncertainty, and where does
the effect live (which users, which weeks). Path analysis is good at proposing
hypotheses — a loop that exists in one arm only, a cluster that converts
differently — and an experiment is how they are confirmed, so the two belong in
one library.

Four independent reviews of the first plan found that the statistics were
sound but the contracts were not: the existing `diff` splits *events* by
segment, so a path whose segment changes falls in both arms; an
intention-to-treat population had no source; per-period rows on "active users"
compare survivors, not randomised groups; a verdict that read an unadjusted
interval next to an adjusted p-value could disagree with itself. The decisions
below are the answers.

## Decision

**D1 — names.** The scorecard is `Eventstream.get_ab_test`, the replicate
table `get_ab_bootstrap`, the per-path table behind the scorecard
`get_ab_paths`, the SRM check `get_sample_ratio`: getters, like
`get_conversion_rate`, whose reference pages are rendered from their docstrings
in a Tools reference section (ADR-0013), with a short section each in the
Eventstream guide. A `*_data` name is reserved for the headless twin
of a widget (ADR-0006); when an `ab_test` widget ships, `ab_test_data` becomes
its twin and an alias of `get_ab_test`.

*Shipped with the first release of these tools:* `Eventstream.ab_test` (widget:
scorecard table and forest plot, Strata and Periods tabs, the metric editor of
ADR-0007 in the sidebar) and `ab_test_data`, a wrapper around `get_ab_test`
whose docstring is `get_ab_test`'s with an alias note. The widget's data
parameters are `get_ab_test`'s, and its numbers are `ab_test_data`'s rows
unchanged: the Scorecard is the call without `strata_col` and `period`, the
Strata and Periods tabs the calls with each. `get_ab_test` stays the name the
docs use.

**D2 — path-level groups.** `tools/_diff.py::resolve_diff` resolves a `diff`
into two disjoint sets of path ids (`DiffGroups`). The inferential methods
that compare two arms (`get_ab_test`, `get_ab_bootstrap`,
`get_conversion_rate(diff=)`, `funnel(ci_level=)`) refuse a path that would
land in both groups with `DiffConfigError`, counting them and pointing at a
finer `path_col` or a path-level segment (`add_segment(path_values=)`).
`get_sample_ratio` does not refuse such paths: it counts them as
`multi_valued_paths` and raises `mismatch`, because for a sample ratio check a
path that carries two arms is the finding, not an input error. The
existing widgets keep their event-level split; migrating them is a later,
announced behaviour change. An `assignment=` table (path id → arm, optionally
`assigned_at`) defines the intention-to-treat population: silent paths count as
zeros, events before assignment are dropped, and a path grain finer than the
table's unit is refused, because per-session intervals of a per-user experiment
are too narrow (Deng et al. 2018). Without a table the segment form of `diff`
gets the same guard: when the arm is constant within at least 99 % of the paths
of a coarser path column that hold several finer paths (and at least 10 such
paths), the finer `path_col` raises `InvalidParameterError`, and from 90 % it
warns; a share rather than "every user", so that a few users carrying both arms
do not switch the guard off. `get_ab_test`, `get_ab_bootstrap`,
`get_sample_ratio`, `funnel(ci_level=)` and `get_conversion_rate(diff=)` apply
it.

**D5 — periods.** `period=` rows are intention-to-treat per period by default:
every path assigned by the period's end is in the period, active or not.
`period_population="active"` rows compare survivors and are labelled
`descriptive`, without a p-value. `period_basis="since_assignment"` bins events
by time since each path's assignment and keeps a path in a bin only when the
bin's window closed before the data ends (`immature_paths`), which is how an
effect that changes with exposure age (novelty, for one) is measured: as a
contrast of since-assignment windows in
`get_ab_bootstrap(contrast=...)`, with a joint interval. `horizon=` gives every
path the same observation window.

**D6 — multiplicity and the verdict.** The family is the set of rows of one
metric (its strata and periods); metrics are separate families by default
(`correction_scope="metric"`), and `correction_scope="all"` makes every row of
the call one family, for a scorecard read without one decision metric. The docs
ask for one decision metric declared in advance instead of a correction across a
whole scorecard, and for guardrails read each on its own against a harm margin
declared before the test: correcting guardrails together raises each one's bar
for `decrease` and makes harm harder to detect (Spotify's risk-aware decision
rule corrects the decision metrics, not the guardrails). A margin-based verdict
is deferred; until then the docs compare the interval with the margin. `correction` is
Holm by default, Bonferroni (which adds simultaneous intervals), Benjamini–
Hochberg for many exploratory rows, or none; `correction_family` names what was
corrected together. The `verdict` (`increase` / `decrease` / `inconclusive` /
`descriptive` / `insufficient_data`) is a pure function of the adjusted
p-value; the interval is per comparison and documented so; `mde` is computed at
the Bonferroni level `alpha / m` (`mde_alpha`), which is not the verdict's
threshold. The test follows the metric kind: pooled
z-test with Wilson / Newcombe intervals for 0/1 metrics, Welch for per-path
totals and values, a joint path bootstrap for ratios of sums (multinomial
weights within arm, batched, seeded per arm, percentile interval, p-value from
the bootstrap standard error; Bonferroni's simultaneous interval is normal on
that standard error, since a percentile at `1 - alpha / m` rests on the last
few replicates). A support rule (`min_support`, counted in
supporting paths, not events) marks rows too thin to test; support is pooled across the arms, so a behaviour present in one
arm only still gets a p-value, flagged in `support_warning` (a per-arm rule
hid exactly the effects a new feature creates, and a guardrail that wipes out
an outcome in one arm). Strata carry shares by arm, a share-balance check (not
a guard against post-exposure strata), Cochran's Q over strata that pass a
support guard, a stratum-versus-rest test and an `exploratory` flag. Whether a
stratum is pre-exposure is declared (`strata_pre_exposure=True`, for a source
column such as a signup platform, or `add_segment(path_values=...,
pre_exposure=True)`, which records the declaration with the segment) or read
from a truncation in the recipe, and recorded in `strata_pre_exposure_source`;
any other stratum warns and is flagged `strata_pre_exposure=False`. The library
has no notion of an exposure marker: `"recipe"` means that a truncation that
ends paths at an event preceded the labels, and the library cannot verify that
it ends at exposure, so the docs ask the analyst to declare a cut the analysis
relies on. Permutation tests are not part of the scorecard. Every row carries a
`reading`, one sentence built from the row's own numbers for people and agents
who quote it: its window first, the per-comparison interval, the p-value after
the correction, the design's `mde` with the paths a new test would need, then
the row's flags; a row with a validity flag does not claim that effects outside
its interval are unlikely, and the sentence does not check the sample ratio or
the unit.

**D8 — funnel intervals.** `funnel(ci_level=, correction=)` adds Wilson
intervals per step and, in diff mode, Newcombe intervals and pooled-z p-values
of the overall-rate deltas with an optional correction over steps. Conditional
step deltas get intervals but no p-value and are never coloured: their
denominator is a post-exposure subset. The widget draws whiskers and colours a
delta only when its interval excludes 0 and, under a correction, its adjusted
p-value is below alpha; NaN reaches the browser as `null` in every widget. The relative delta's base is group 2.

Common rules: an absent estimate is NaN, never 0; `inconclusive` is never
spelled "no effect"; the unit of resampling is `path_col`; `tools/_stats.py`
and `tools/_bootstrap.py` are pure functions without an `Eventstream`.

## Consequences

- Users get an experiment readout without leaving the library, and the same
  numbers through the MCP server: `get_ab_test`, `get_ab_bootstrap`,
  `get_sample_ratio` and `get_conversion_rate` return numbers only, and
  `add_ab_test` registers the scorecard as a report tab (the `ab_test`
  widget).
- Two `diff` semantics coexist until the widgets migrate: event-level in the
  descriptive widgets, path-level in the inferential tools. The docs say which
  is which.
- Pre-assignment comparisons (an A/A strip before assignment) are not produced
  by `get_ab_test`, which drops events before `assigned_at`; they are built on a
  separate stream of the events before each path's `assigned_at` (a pandas filter
  on the event table: `truncate_paths` cannot anchor on a per-path timestamp
  column).
- The tutorial's pre-assignment A/A strip is deferred: the bundled
  `load_ab_test()` dataset assigns users at their first app open and has no
  pre-history, so there is nothing before assignment to compare. A future
  revision of the dataset may plant a few days of pre-assignment history; the
  generator is unchanged in this release, and the tutorial says why the check
  is missing.

## Roadmap (not in this release)

1. Behaviour in A/B: ~~a transition test per edge with Benjamini–Hochberg over
   edges and a shared vocabulary~~ shipped (see the amendment below); then
   dose-response; a `prob_group1_better` column.
2. Design from history: an A/A replay on past data to size a test and check
   the false-positive rate of the planned analysis, then `get_ab_design`.
3. Variance reduction: CUPED and linearisation on pre-exposure metrics. There
   is no `covariates=` parameter meanwhile; `get_ab_paths` returns the per-path
   table behind the scorecard for an adjustment by hand.
4. Populations: `exposure_event=` / `population="exposed"`, a balance check
   with a studentised within-strata permutation test.
5. ~~An `ab_test` widget (forest plot, strata and period panels) with the
   `ab_test_data` twin.~~ Shipped (see D1).

Deferred decisions: `assignment=` on the funnel, and equivalence /
non-inferiority verdicts (a margin-based verdict next to the superiority one).
Items 1–3 are ordered by expected value per effort: the transition test first,
design-from-history second, variance reduction third.

## Amendment (2026-10): the transition and step screens

Roadmap item 1 ships as `Eventstream.get_ab_transitions(diff, edge_weight=...)`
and `get_ab_steps(diff, max_steps=...)`, with MCP tools of the same names and a
significance overlay on the diff transition graph
(`transition_graph(diff=..., ab_transitions=frame)`).

**A1 — one machine, not a second scorecard.** Both screens reuse the scorecard's
contracts: `diff` resolved per path, the population (`assignment=` for ITT,
silent paths included), the unit guard, `horizon` with `immature_paths`, the
pooled support rule, the verdict as a pure function of the adjusted p-value, the
`reading` sentence, `mde` at `alpha / m`, and the z-test, Welch and joint path
bootstrap. The shared rules moved out of `get_ab_test` into helpers in
`tools/ab_test.py` (validation, population, analysed window, support, adjustment,
verdicts), and the scorecard's numbers did not change. Per-path edge counts come
from the transition graph's own SQL (`path_transition_counts`) as a sparse
matrix; the bootstrap takes sparse input and gives bit-identical replicates.
An arm's value equals `transition_graph_data` / `step_matrix_data` for that arm
on the stream's paths, for every weight.

**A2 — a screen with FDR control.** The family is every edge (cell) with enough
pooled support and a p-value, adjusted together with Benjamini–Hochberg by default
(`correction_family` names it, `attrs["n_tested"]` sizes it). A row outside it —
below the support rule, or without a p-value (constant in both arms, invalid
bootstrap replicates) — is `insufficient_data`, so the tested verdicts add up to
`n_tested`; the scorecard, which has no family size to agree with, reads a
supported row without a p-value as `inconclusive`. It is found after
looking, so it is exploratory by construction: a discovery's reading ends "a
hypothesis to confirm as the declared metric of a new test, not a decision".
`edges=[(source, target), ...]` declared before the test makes a confirmatory
family. Intervals are per row; only Bonferroni adds simultaneous ones, which
for bootstrapped rows are normal on the bootstrap SE (a percentile at
`1 - alpha / m` is beyond `n_bootstrap` replicates at a screen's `m`), so they
agree with the adjusted p-value, the same rule as the scorecard's bootstrap
rows.

**A3 — per-path quantities, conditional ratios.** `unique_paths` (0/1, z-test)
and `avg_per_path` (count, Welch) average over every path of the arm and are
effects of the assignment. `proba_out`, `proba_in` and `share_of_total` are
ratios of sums over paths (joint path bootstrap of numerator and denominator
means on shared weights), and their denominators are post-treatment behaviour:
every such row has `conditional=True` and its reading says what it compares.
`count` raises (it grows with the arm's size); the time weights are not tested.
Step cells are 0/1 per path from each path's start; `path_end` cells count the
paths that ended, silent paths ended at step 1; step 0 is not a cell.

**A4 — the overlay is the frame.** The graph takes the frame itself (no `_data`
twin): it is validated against the graph's `diff`, `edge_weight` and
`path_col`, shipped as a JSON payload, exported in static HTML, and not
persisted in `state_file`. Significant edges are drawn in their sign's colour and
bypass the edge filter; inconclusive edges turn grey, untested ones dashed, edges
without a row faint, and the tooltip shows the test's values (shares for
`unique_paths`) so its units agree with the test. The label of an edge with a row
is the frame's delta, and every row on an edge of the graph is drawn even where
the graph's own difference is 0: with arms of different sizes a path-count
difference can be 0, or have the other sign, while the shares differ. When the sidebar changes the
weight, the diff or the path column, the overlay steps aside.

Validation: on `load_ab_test()` the screens find the planted cart loop (story 4)
and, with `proba_out`, the conditional checkout step (story 2) flagged
conditional; over 20 `aa_calibration` seeds no run had an edge discovery for any
tested weight, and the raw null p-values were uniform.

Out of scope in this release: time weights (`time_median`, `time_q95`), `count`,
`path_pattern` / `anchor` on steps (cut with `truncate_paths`), simultaneous
intervals other than Bonferroni, a confidence interval on `rel_delta`.

## Amendment (2026-10): the screens as matrices

The two screens get widgets of their own, `ab_transitions(diff, edge_weight)` and
`ab_steps(diff, max_steps)`, with the `_data` twins `ab_transitions_data` and
`ab_steps_data`: documented aliases of `get_ab_transitions` / `get_ab_steps`, as
`ab_test_data` is of `get_ab_test` (ADR-0006, D1). Each cell of the matrix is one
row of the twin's frame, sent as it is; the widgets compute nothing. The rows travel
column by column, a value repeated down a column once: 80 event types are 6,561
rows with a long `reading` each, about 8 MB as one object per row and under 2 MB
this way, every cell kept.

**M1 — one encoding.** The background is the delta on a diverging scale around 0
in the diff palette (red where group 1 is higher, blue where it is lower: the
graph overlay's and the diff step matrix's hues), its full colour at the largest
`|delta|` shown or at a clip the reader sets. A wide inset outline reads
`p_value_adjusted`: black below alpha; light grey with a light grey cross when
tested and not significant; dashed, with the cross and muted text, when the row
has no adjusted p-value; nothing when the frame has no row. The outline reads the
column, not the verdict's wording, so it does not depend on how an untested row's
verdict is spelled. Intervals stay per cell; with Bonferroni the simultaneous ones
can be shown instead.

**M2 — what the reader varies.** The sidebar recomputes the weight (or
`max_steps`), the correction, the level and `min_support`; the other arguments
are fixed at construction and shown read-only. View controls (intervals shown,
significance filter, colour clip, sorting, pinned and hidden events, cell size)
never recompute and are saved with `state_file`; `diff` and `assignment` are not
persisted. The static export draws the same matrix without a kernel.

**M3 — tables for export.** `get_ab_transitions_table` / `get_ab_steps_table`
take the getter's arguments plus `values=` (frame columns, validated) and
`styled=`, and pivot the frame into the diff matrix shape: index the sources
(events), columns `(target, stat)` (`(step, stat)`), NaN where the frame has no
row, the frame's `attrs` plus `values`. They are named `get_*` because they return
numbers (ADR-0008 rule 6); a bare noun would read as a widget. `styled=True`
returns a pandas `Styler` with the M1 encoding in CSS that `to_excel` keeps;
`openpyxl` stays optional. The encoding's colours and opacity are constants of
`tools/ab_tables.py`, from which the widgets' are generated
(`make export-ab-encoding`, checked by a test), so the table and the matrix
cannot drift apart.

The MCP server is unchanged: ADR-0009 asks that renames reach the server and
that tool results stay compact; nothing is renamed, and the screens' MCP tools of
the getters' names already return compact summaries. A report tab for the
matrices is not part of this change.

## Amendment (2026-10): data sufficiency and vocabulary compression

The screens are data-hungry: every edge (cell) is its own test, a row below
`min_support` is not tested, and the family grows with the vocabulary. Two answers
are added, all in the A/B group except one general processor.

**C1 — how much data a screen needs.** `get_ab_transitions_power` and
`get_ab_steps_power` take the screens' arguments plus `scale` (multiples of the
current paths, both arms alike; 1 always included) and `target_delta`, and return
one row per (row of the screen, scale). The projection is the screen's own
machinery with the paths multiplied: the support rule on the projected pooled
support, the family `m` = the rows testable at that scale, and the `mde` at
`alpha / m` (the screen's MDE level: Bonferroni, conservative for BH, which never
rejects below `alpha / m`) with the standard error scaled by `1 / sqrt(scale)` —
the pooled share at the projected arm sizes for a z-test row, `delta_se /
sqrt(scale)` for a Welch or bootstrapped row. At `scale=1` every number equals the
getter's (tested). With `target_delta`, the paths per arm a row needs are those at
which it passes the support rule and its MDE reaches the target; a rare row's
small MDE does not count before it is testable. `attrs["curve"]` sums each scale up
(cells, testable, family, MDE median and quartiles, share reaching the target).
The projection assumes the same path composition, and the docs say it sizes a new
test, never a longer run of this one. The matrix widgets get a **Data needed**
view (the cell background the MDE or the users a target needs, untested cells
hatched with `×k to test`, the curve as two small panels — testable cells and
median MDE against the users, never one chart with two scales); its colours are
constants of `tools/ab_tables.py`, generated into the widget like the delta
encoding. The payload carries the curve and each row's `scale_to_test`; the
users a target needs, `max((mde / target)^2, scale_to_test)`, are computed in the
browser from the row's `mde`, so the target is view state and needs no recompute.

**C2 — a smaller vocabulary, chosen without the arms.**
`get_ab_compression(diff, screen="transitions" | "steps", ...)` returns a plan:
one row per event (`action` keep / merge / drop / other, the new `label`, the
evidence and a `reason` in words) and, in `attrs["path"]`, the greedy path — one
operation per step with the screen's numbers after it (vocabulary, cells,
testable and untestable cells, median MDE, transition mass retained, artificial
transitions). The operations: merge a chain (`P(next)` and `P(prev)` both at least
`chain_threshold`); drop a rare event when the screen's bridge rule holds, else
relabel it `other`; merge synonyms (Jensen–Shannon distance of the in- and
out-transitions, no self-loops of their own) only when one is thin and the merge
leaves fewer untestable cells and loses no testable one. `keep` events,
`path_start` / `path_end` and a declared `edges=` family are never touched.

Arm-blind: every statistic is pooled; the plan reads the two arm sizes (for the
MDE) and never a path's arm, so permuting the arms or swapping them in `diff`
leaves it identical (tested). Choosing the vocabulary from arm differences would be
selection on the outcome; filtering on the pooled support, which does not depend
on the arms under the null, is independent filtering and keeps BH valid (Bourgon,
Gentleman and Huber, 2010). The plan's statistics read each path's whole window,
not the first `max_steps` steps: the plan applies to the stream.

Deviations from the first design, with their reason:

- `target="testable"` maximises the *count* of testable cells (ties: the fewest
  untestable cells, the lowest median MDE, the earliest step), not their share: a
  share rewards a tiny vocabulary with few cells, all testable.
- Synonym merges are gated: one member must be thin (at least half of the paths
  through its cells in untestable cells) and the merge must reduce the untestable
  cells without losing a testable one. Ungated, two well-measured events with
  similar neighbours (`catalog` and `search`) merged and lost real cells.
- `screen=` takes the other screen's arguments as an error (`max_steps` with
  `"transitions"`, `edge_weight` / `edges` with `"steps"`), not silently.

**C3 — one plan per screen.** The transition screen wants fewer, richer edges
and no invented transition: a rare event is dropped only when, for
`bridge_threshold` of its runs, the events around it already follow each other
directly in at least `min_support` paths; chains come first. The step screen
wants aligned steps: a rare event is dropped when it is an insertion (the paths
without it go from the event before to the event after directly at least as
often as through one other event), which realigns the later steps; otherwise it
becomes `other`, which keeps every position; rare events come first, chains last.
On Northwind with `min_path_share=0.1`, `support_chat` is dropped by the
transitions plan and relabelled by the steps plan (a test pins the reason).

**C4 — homogeneity.** `by=<segment fixed before exposure>` builds a plan per
segment (the segment column on every row, `chosen_step` and `mappings` per
segment), with `attrs["homogeneity"]` per segment: effective vocabulary
`exp(entropy)`, transition entropy, distinct-path share, and the numbers before
and after. The pre-exposure rule is the scorecard's: `by_pre_exposure=True`, or
the recipe shows it; otherwise it warns. Within a segment the arm sizes are the
segment's.

**C5 — applying a plan.** `compress_events(plan)` is a general data processor (a
verb, ADR-0008) and adds no transformation: it is `edit_events(delete, rename)`
followed by `collapse_events(loops=[merged labels])` (ADR-0003: a new stream, the
step in the recipe, a plan frame recorded as its rows). It takes the plan frame,
its records, or a `{event: label | None}` mapping. The compressed stream's
screens reproduce the plan's step in `attrs["path"]` (tested on every step of a
toy stream and on Northwind).

**C6 — the widget and MCP.** `ab_compression(diff, screen, ...)` with the twin
`ab_compression_data` (an alias of `get_ab_compression`) draws the path as three
panels sharing the vocabulary axis (testable cells, median MDE, mass retained;
artificial steps ringed), a step slider, the mapping at the selected step and the
numbers before and after. The payload holds every step (per event, the steps at
which its label changes), so the slider works in a static export. The matrix and
compression widgets share one base (`widgets/_ab_widget.py`). Through MCP,
`get_ab_transitions_power`, `get_ab_steps_power` and `get_ab_compression` are
compact-result tools (ADR-0009): the curve and the rows nearest to testable; the
plan's numbers before and after, the path up to the chosen step, the changed
events and a ready `compress_events` step for `local_preprocessors`.
`compress_events` is a documented preprocessor.

Measured: on the bundled experiments the default plans keep five of six streams as
they are (they are dense) and merge `session_start+home` on `session_assignment`
(57 of 59 step cells testable before, 65 of 66 after); no default plan creates an
artificial transition. On 40,000 and 200,000 simulated users of a 79-event
store, the projection from 40,000 to ×5 predicted 1,386 testable edges at a median
MDE of 0.106 pp, and the 200,000-user sample had 1,375 at 0.106 pp.
