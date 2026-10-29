<!-- retentioneering MCP playbook — loaded by describe_tool() and playbook() -->
<!-- Each ## heading is a scenario key. Content is returned as-is to the agent. -->

## temporal_anomaly

**Scenario:** Spike, drop, or incident on specific dates

**Trigger:** User asks about a specific date range: "why did X happen on Jan 20–22?",
"analyse the spike/drop", "what happened during that period?".

**Steps:**

1. Call `describe()` — note `timestamp_col` and `date_range`.
2. Confirm with the user, then call `update_base_stream` with:
   ```
   preprocessors=[{
     "type": "add_segment",
     "name": "period",
     "rules": [
       ["{timestamp_col}", "<", "YYYY-MM-DD", "normal"],
       ["{timestamp_col}", ">", "YYYY-MM-DD", "normal"],
       ["anomaly"]
     ]
   }]
   ```
   Replace `{timestamp_col}` with the value from `describe()`.
   First date = start of anomaly window, second date = end.

3. Call `add_transition_graph` and/or `add_step_matrix` with `diff=["period","anomaly","normal"]`.
4. Call `add_segment_overview(segment_col="period")` to compare all KPIs.

**Rules format:** each entry is `[column, op, value, label]`; the last entry `["anomaly"]` is the ELSE fallback.

**Concrete example** — spike on Jan 20–22 2024, timestamp column is `timestamp`:
```json
{
  "type": "add_segment",
  "name": "period",
  "rules": [
    ["timestamp", "<",  "2024-01-20", "normal"],
    ["timestamp", ">",  "2024-01-22", "normal"],
    ["anomaly"]
  ]
}
```
This labels every event row: those outside Jan 20–22 get `"normal"`, the rest get `"anomaly"`.
Use `timestamp_col` from `describe()` as the column name.


## conversion_dropoff

**Scenario:** Conversion drop-off — why is conversion from A to B falling?

**Trigger:** User asks about conversion between two events or a multi-step funnel.

**Steps:**

1. Confirm with the user, then call `update_base_stream` with:
   ```
   preprocessors=[{
     "type": "add_segment",
     "name": "funnel",
     "funnel_events": ["A", "B"]
   }]
   ```
   For a 3-step funnel use `["A", "B", "C"]`.

   This is a strict, ordered funnel — a step only counts if every earlier step also
   occurred first, in order. Segment levels are named after the **deepest step
   completed in order**:
   - `out_of_funnel` — never completed A (or only reached B before A)
   - `A` — completed A, but B either never happened or happened before A
   - `B` — completed the funnel, A before B

2. Call `add_transition_graph` and/or `add_step_matrix` with `diff=["funnel","A","B"]`.
   For a 3-step funnel, choose which drop-off to analyse:
   - A→B drop-off: `diff=["funnel","A","B"]`
   - B→C drop-off: `diff=["funnel","B","C"]`

**Example:**
```json
{"type": "add_segment", "name": "funnel", "funnel_events": ["add_to_cart", "purchase"]}
```
Then: `add_transition_graph(label="Funnel", diff=["funnel","add_to_cart","purchase"])`


## segment_comparison

**Scenario:** How do two groups behave differently?

**Trigger:** User asks about differences between segments (new vs loyal, mobile vs desktop, etc.)

**Steps:**

1. Call `describe()` — check `segment_cols` for the relevant column.
   No preprocessing needed if the column already exists.
2. Use `diff=[segment_col, value1, value2]` in `add_transition_graph` / `add_step_matrix`.
3. Call `add_segment_overview(segment_col=...)` for a full KPI comparison across all segment values.


## funnel_analysis

**Scenario:** What happens between event A and event B?

**Trigger:** User asks about the path from one event to another.

**Steps:**

Use `add_step_matrix(path_pattern="A->.*->B")` to focus on sessions that pass through
both anchor events. The `->.*->` matches any intermediate events.

`add_transition_graph(path_pattern="A->.*->B")` takes the same pattern, but only
*selects* the paths — a graph has no step axis to centre, so nothing is cut. To cut,
use `truncate_paths`.


## conversion_rate

**Scenario:** Does one event actually lead to another?

**Trigger:** User asks how likely something is after something else — "do people who
hit the error come back?", "how many go from cart to purchase?", "what share leave
right after X?", "is landing on this page good or bad?".

**Steps:**

1. `get_conversion_rate(start_anchor="Y", end_anchor="X")` — one call, no preprocessing.
   Add `within=10` (events) or `within="30m"` (time) when the question has a window;
   pass `end_anchor=["X1", "X2", "path_end"]` to ask about several outcomes at once.
2. Read three numbers, not one:
   - `paths_with_start` — how many paths the rate is even about;
   - `conversion_rate` — the answer;
   - `lift` — the answer against the baseline. **Below 1 means Y makes X LESS
     likely**, which is usually the finding worth reporting.
3. Quote the figures in backticks (`` `30.0%` ``) — this tool adds no tab, so its
   numbers are agent-computed and exempt from the anchor-link rule.

**Choose the unit deliberately.** Per user (`path_col` default) a conversion is diluted
by everything that person ever did; per visit (`path_col="session_id"`, if `describe()`
lists it) it describes the session at hand. The same pair can look neutral per user and
strongly negative per session.

**Idioms:**
```
end_anchor="path_end", within=1                             — exit rate after Y
start_anchor={"pattern": "path_start->Y", "at": -1}         — only paths that STARTED on Y
start_anchor="Y", end_anchor="Y"                             — how often Y repeats
```

**Do not read this off a transition graph.** A graph edge is a share of *transitions*
between adjacent events; this is a share of *paths* where one event was followed by
another at any distance. Both are correct and they answer different questions.

**Not answerable this way:** anything per-occurrence ("of 23,000 visits to this page,
how many were entrances"). The unit here is the path — a path where Y happened three
times counts once. For a per-visit figure, carve out the subpopulation with
`filter_paths` and read a transition matrix instead.


## ab_test

**Scenario:** Did the treatment of an A/B test work?

**Trigger:** User has an experiment arm column and asks whether a variant won, lost or
"did nothing", or asks for an experiment scorecard.

**Steps:**

1. `get_sample_ratio(segment_col="<arm col>")` — before any metric. Quote its
   `reading`. `mismatch=true` (arms off the planned split, or paths carrying two arms)
   means the assignment or the logging is broken; report that and stop. With
   `period="D"` or `strata_col="<segment fixed at assignment>"` the `reading` also
   names the days or strata that mismatch — a mismatch in one platform or from one day on voids the test
   even when the overall split looks fine.
2. Before the first `get_ab_test` call, ask which metric decides and which metrics
   are guardrails, each with the largest harm it may do (e.g. revenue per user must
   not drop by more than 5 %). If the user names no decision metric, report every
   row as supporting evidence and say that no decision metric was declared. Then
   `get_ab_test(diff=["<arm col>", "treatment", "control"], metrics=[...])` with
   those metrics, each with a `name` (arguments under `metric_args`;
   `describe_tool("metrics")` lists the registry, `describe()` the arm names in
   `segment_levels` and the money columns in `value_columns`):
   ```json
   [{"name": "conversion", "metric": "has_event", "metric_args": {"event": "purchase"}},
    {"name": "revenue", "metric": "column_sum", "metric_args": {"column": "amount"}},
    {"name": "aov", "metric": "column_sum", "metric_args": {"column": "amount"},
     "denominator": {"metric": "event_count", "metric_args": {"event": "purchase"}}}]
   ```
3. Report per metric: `delta` with `[delta_ci_low, delta_ci_high]`, the `verdict` and
   the `mde`. Quote in backticks (no tab is registered). Every row has a `reading`:
   one sentence built from its numbers. For an `inconclusive` row, quote its `reading`
   verbatim (in backticks) — it says what the interval includes, the p-value after
   the correction, what the design detects and how many paths a new test needs for a
   smaller effect — and then, if useful, propose a next step (below). The `reading` starts with the window
   it covers ("over the whole observation period", "calendar week 2026-03-09",
   "days 14-27 since assignment"): a period row is not the test's result, so
   always name the window when you quote a number. With several metrics each
   reading ends "corrected within this metric only; name one decision metric": only
   the decision metric declared in step 2 can be called a win; never pick it after
   seeing the rows. A guardrail is read alone: it passes only when its interval
   clears the declared margin (a grey guardrail whose interval reaches past the
   margin has not passed). `correction_scope="all"` is for a scorecard read without
   one decision metric, never for guardrails; every row's `scope` says what it is
   corrected against. `unit` says whether
   the reading is in percentage points (`pp`, a 0/1 metric whose `delta` fields
   are shares) or in the metric's own unit (`value`).
4. Optional: `strata_col=` for a segment fixed before exposure (platform at signup),
   `period="W"` for stability over time. Strata rows are adjusted with `correction`
   (`strata_correction_family`); `exploratory=true` (or `strata_pre_exposure` not
   true) means the strata may be post-exposure: report them as exploratory.
5. Time: with the assignment table, `horizon="28D"` reads each user's first 28 days
   (choose the window before reading: different horizons answer different
   questions), and `period="14D", period_basis="since_assignment"` gives rows by
   days since assignment. `get_ab_bootstrap(..., contrast={"novelty": [["0-13"],
   ["28-41"]]})` puts one interval on "the early effect minus the late one".
6. Finish: `export_report` needs at least one tab, and the numbers-only tools
   register none. Add the scorecard as a tab with `add_ab_test(label=..., same
   arguments)` (or an `add_segment_overview` by arm) before `check_analysis` and
   `export_report`.

Population (`population` on every row): with an assignment table in the session
(`serve(stream, assignment=...)`; `describe()` shows `assignment`) every assigned
user, silent ones included (intention to treat, `"assignment"`), and `horizon` /
since-assignment periods work. Without one, the stream's paths (users with at least
one event) split by the arm segment, over the whole window: silent assigned users
are not counted, there is no horizon, and the funnel and the scorecard can differ
from an ITT read. Say which population a number is about. Quote `warnings` if the
call returns any.

**A treatment that renames an event:** when the treatment logs a new event instead of
an old one (`quick_add` instead of `add_to_cart`), a metric on the old event alone
shows a huge, meaningless drop (`support_warning="one_arm_only"` on the new one).
Compare the union: `{"metric": "has_any_event", "metric_args": {"events":
["add_to_cart", "quick_add"]}}`.

**Who does it help** (in this order): (1) SRM gate (`get_sample_ratio`, also by
`period="D"` and by a signup stratum); (2) the scorecard on the decision metric;
(3) strata declared before looking — segments fixed at signup
(`segments_constant_per_path` in `describe()`) with `strata_pre_exposure=true`, and
read `heterogeneity_p_value` before any single stratum; (4) only then descriptive
looks at loops and conditional steps (`add_transition_graph` with `diff`,
`get_conversion_rate`), labelled descriptive. A stratum found by looking is a
hypothesis for a targeted follow-up test, not a result.

**Reading the verdict:** `increase` / `decrease` come from the multiplicity-adjusted
p-value: `p_value_adjusted` below `1 - ci_level` (`mde_alpha` is only the level the
`mde` is computed at; do not compare p-values with it).
`inconclusive` is not "no effect": report the interval as the range of effects the
data still allow, by quoting the row's `reading`. The `mde` is the effect the test detects with 80 % power at
`mde_alpha`, a design quantity: never say the effect is smaller than the `mde`.
`insufficient_data`: pooled support (paths, not events) below `min_support`; report its estimates
descriptively. `support_warning="no_data"`: no path of either arm has the
behaviour — say "no data in either arm", not "one arm only" — or a value / ratio
metric has no value in one arm (the reading says which). `conditional=true`: the
metric exists only for some paths (`time_between`, `column_mean`, order value), so
the row compares those paths (`group1_support` of `group1_paths`), not the arms —
never report it as the treatment's effect on everyone. `support_warning="one_arm_only"`: the behaviour exists in one arm only;
the test stands, say so next to it. `support_warning="invalid_replicates"`: a
bootstrap row with no interval or p-value; report its estimate descriptively.
`support_warning="low_support"`: a bootstrap row under 30 supporting paths per arm;
its interval covers less than nominal, so call it tentative. `descriptive`: rows of
`period_population="active"` (survivors), no p-value. Strata are the top 3 by |delta|
(`strata_selection`): exploratory unless `strata_pre_exposure` is true.

**Next step after an inconclusive row** — always a NEW test, designed before it
starts; never this test run longer, re-read at another horizon or re-sliced until a
row flips (that is peeking):

- **More paths for the next test** — the `reading` says how many times the paths a
  smaller effect needs ("to detect 1 pp, about 5.7× the paths are needed in a new test
  sized before it starts"): size the next test for the effect that matters to the
  business, with its horizon and read date fixed up front.
- **A horizon declared for the next test** — when the metric arrives late (a trial
  that converts after 14 days, `period_complete=false` on the last periods, "window not
  complete" in the `reading`), the next test declares its observation window
  (`horizon=`) before it starts. A horizon tried on this test after seeing the result
  does not count.
- **A pre-declared stratum** — when the effect plausibly lives in one segment fixed
  before exposure (a platform at signup), a test targeted at it, with the stratum
  declared before it starts; a stratum picked because its delta looked large is
  exploratory and needs its own test.
- **A cheaper metric** — when the outcome is rare (a low base rate gives a large `mde`),
  the next test can decide on a more frequent behaviour closer to the change (adds to
  cart instead of orders) as a leading indicator, declared up front.

Never turn an inconclusive row into "no effect", and never rerun until a row flips.

**Do not:** filter on post-exposure behaviour ("users who reached checkout") before
the test, or stratify by it — that compares selected groups, not the randomised arms.
Use the randomised unit as `path_col` (per-user experiment → user paths).


## comparable_windows

**Scenario:** Why did one group convert and another not, given both got as far as B?

**Trigger:** User asks what distinguishes converters from non-converters, or any
"group X did the thing, group Y didn't" comparison.

**Why this needs care:** the two groups are censored differently. Converters have
events after the conversion; non-converters simply run to the end of their path. A
diff over the raw paths then measures *how much path each group has*, not how they
behaved. Cut both groups to a window that is defined the same way for each.

**Steps:**

1. Label the groups:
   ```json
   {"type": "add_segment", "name": "funnel", "funnel_events": ["A", "B", "C"]}
   ```
2. Keep only paths that got as far as the shared milestone, then cut every path to
   the same window — at C, or the same number of events past B, whichever is sooner.
   The non-converters have no C, so the step bound is what applies to them:
   ```json
   {"type": "filter_paths", "matches_pattern": "A->.*->B"}
   ```
   ```json
   {"type": "truncate_paths",
    "start_anchor": {"pattern": "A->.*->B"},
    "end_anchor": ["C", {"pattern": "A->.*->B", "offset": 10}]}
   ```
3. `add_transition_graph(diff=["funnel","C","B"])` — `C` is the segment level for
   paths that completed the funnel, `B` for those that stopped at B.

**Choosing the offset:** it should be close to how long converters actually take
from B to C, and the conclusion should survive changing it. If flipping it between
5 and 15 changes the story, the story is about the window, not the users.

**Caveat worth stating in the analysis:** a path whose B falls near the end of the
logged period is labelled "did not convert" without having had the chance. If the
data ends soon after many B's, say so rather than reporting the difference as
behavioural.


## noise_removal

**Scenario:** Clean data before analysis

**Trigger:** Data has very short sessions, irrelevant events, or repetitive self-loops.

**Steps:**

Confirm with the user, then call `update_base_stream` with one or more of:
```
{"type": "collapse_events", "loops": true}       — removes A→A→A loops
{"type": "filter_paths", "op": ">", "metric": "length", "value": 3}
{"type": "filter_events", "drop": {"event": ["noise_event"]}}
```
