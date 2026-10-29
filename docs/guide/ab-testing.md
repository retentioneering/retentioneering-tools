# A/B Testing

An experiment tells you whether a change moved a metric; path analysis shows how behaviour changed and suggests why. This page explains how retentioneering reads an A/B test, which of its numbers are tests and which are descriptions, and the mistakes that make a comparison of paths look like an experiment result when it is not.

## Why paths and experiments belong together

Path analysis is good at finding hypotheses: a loop that exists in one arm only, a group of users who behave differently, a funnel step where the two arms part ways. It cannot confirm them, because every path view compares whoever happened to do something, and the treatment itself changes who does what. An experiment is how a hypothesis is confirmed — the order the Retentioneering team followed in its [2019 case](https://habr.com/ru/post/468721/), where path analysis found the idea and a split test confirmed it.

The two meet in the other direction too. A test that reads grey on the top line often hides opposite effects in different users, or an effect that fades or reverses after the first weeks, and paths are where you look for them. The tools on this page keep the line between the two visible:

| Inferential: a randomised comparison with an interval and a test | Descriptive: a picture to generate the next hypothesis |
|---|---|
| [`get_sample_ratio()`](/docs/tools/get-sample-ratio) on the assignment or the full stream | `transition_graph`, `step_matrix`, `step_sankey` in [diff mode](/docs/widgets#diff-mode) |
| [`get_ab_test()`](/docs/tools/get-ab-test) on the assignment table, overall and per stratum fixed before exposure (`strata_pre_exposure=True`: declared, such as a platform fixed at signup, or read from a truncation in the recipe) | `get_ab_test` strata not known to be pre-exposure (`exploratory=True`) |
| `get_ab_test(period=...)` rows over every user assigned by the period's end (ITT per period) | `period_population="active"` rows (`verdict="descriptive"`) |
| [`get_ab_bootstrap()`](/docs/tools/get-ab-bootstrap) contrasts, such as a novelty effect | step-to-step funnel rates (`step_conditional=True`) |
| `funnel(ci_level=...)` overall steps | conversion *after* a behaviour the treatment changes, e.g. `get_conversion_rate(diff=...)` from a loop (`conditional=True`) |

## The mental model

Four objects define every A/B number in the library: the unit, the population, the comparison and the metrics.

### The unit is the path, and it must be the randomised unit

Every inferential method treats one path as one independent observation: intervals and tests are computed over paths, and the bootstrap resamples whole paths. That is right only when `path_col` is the unit that was randomised. If users were assigned to arms, the paths must be users; the sessions of one user share the user's arm and habits, so treating them as independent makes every interval too narrow.

With an assignment table the library checks this for you:

```python
stream.get_ab_test(diff, metrics, assignment=assignment, path_col="session_id")
# InvalidParameterError: Invalid value 'session_id' for parameter 'path_col'. Allowed values:
# ["'user_id' — the assignment table lists user_id ids, so user_id is the randomised unit;
#   per-session_id intervals would treat correlated paths of one unit as independent and
#   come out too narrow (Deng et al. 2018)"]
```

Without one (a `diff` on a segment column alone), the library cannot know how the arms were assigned, and the check is yours.

### The population: intention to treat

The population of an experiment is everyone who was assigned, whether they used the feature or not, and whether they came back at all. This is *intention to treat* (ITT): the arms are compared as they were randomised, so nothing the treatment did can change who is compared. Dropping users who never saw the feature, never reached the screen or never returned breaks the randomisation, because the treatment can change who those users are.

An assignment table is the source of that population. It is a DataFrame indexed by path id with a column `arm` and, optionally, `assigned_at` (or a Series `path_id -> arm`):

```python
from retentioneering.datasets import load_ab_test, load_ab_test_assignment

stream = load_ab_test()
assignment = load_ab_test_assignment()
assignment.head(3)
#                arm         assigned_at
# user_id
# 100001   treatment 2026-03-15 03:15:19
# 100002   treatment 2026-03-04 06:24:51
# 100003   treatment 2026-03-10 06:36:16
```

With `assignment=`, every row of the table is in the population. Users who were assigned but never produced an event (*silent* users) count as zeros for 0/1 metrics and totals and are reported in `group1_paths_no_events` / `group2_paths_no_events`; events before `assigned_at` are dropped; paths of the stream that are not in the table are left out and counted in `excluded_paths`. Without `assignment=`, the population is the paths of the stream, which silently drops users with no events.

### The comparison: `diff`

`diff=("exp_group", "treatment", "control")` names the two arms, and every delta is **group 1 minus group 2** — treatment minus control. The inferential methods resolve `diff` per path: a path that falls in both groups (a segment that changes within the path, overlapping id lists) raises `DiffConfigError` instead of being counted twice. With an assignment table, `diff[0]` may be a segment column of the stream, and then every path's segment must agree with the table.

The descriptive widgets keep their event-level `diff` — a path whose segment changes can contribute to both sides — which is the right tool for "inside vs outside an incident window" and the wrong one for an experiment. [ADR-0017](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0017-ab-testing-tools.md) records why the two coexist.

### The metrics: registry configs, one number per path

A metric of the scorecard is an ordinary [path metric](/docs/path-metrics) config — the same dict `get_metrics`, Segment Overview and Cluster Analysis take — with three optional keys: `name` (the row label), `denominator` (a second config that turns the metric into a ratio of sums) and `test` (a per-metric override). The kind of metric sets the test and how silent paths count:

| Kind | Examples | Silent paths | Default test |
|---|---|---|---|
| 0/1 | `has_event`, `has_any_event`, `has_all_events`, `matches_pattern`, `in_segment` | count as 0 | pooled two-proportion z-test, Wilson / Newcombe intervals |
| total per path | `event_count`, `length`, `active_days`, `column_sum` | count as 0 | Welch's t-test |
| value per path | `duration`, `time_between`, `column_mean`, `column_last` | left out of the mean | Welch's t-test |
| ratio of sums | any total with a `denominator`, e.g. revenue / orders | — | joint path bootstrap |

`*_bulk` metrics, `first_event_time` and a `denominator` on a 0/1 metric are rejected. A ratio such as average order value is not a per-path mean: it divides summed revenue by summed orders, and the bootstrap resamples users, so the interval respects that both sums come from the same people.

## A worked example

`load_ab_test()` is "Northwind Home", a synthetic store with a randomised 50/50 test of `quick_add`, a one-tap "add to basket" button on catalog and search tiles. About 10,000 users are assigned at their first app open over six weeks; the data cover eight weeks. The dataset is built so that the analysis below finds the stories planted in it (they are listed in the docstring of `retentioneering.datasets.ab_test`), and nothing else.

**First, the sample ratio.** Before reading any metric, check that the arms have the sizes the design promised:

```python
srm = stream.get_sample_ratio("exp_group", assignment=assignment)
srm["levels"]
#        level  paths  share  expected_share  expected_paths
# 0    control   5040  0.504             0.5          5000.0
# 1  treatment   4960  0.496             0.5          5000.0
srm["p_value"], srm["mismatch"], srm["silent_paths"], srm["multi_valued_paths"]
# (0.4237, False, 267, 0)
```

The split is fine. 267 of the 10,000 assigned users never produced an event; they are in the counts, because the population is the table.

**Then the scorecard**, on a few metrics chosen before looking at the data:

```python
diff = ("exp_group", "treatment", "control")
orders = {"metric": "event_count", "metric_args": {"event": "order_success"}}
metrics = [
    {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order_success"}},
    {"name": "adders", "metric": "has_any_event", "metric_args": {"events": ["add_to_cart", "quick_add"]}},
    {"name": "orders_per_user", **orders},
    {"name": "revenue_per_user", "metric": "column_sum", "metric_args": {"column": "order_value"}},
    {"name": "aov", "metric": "column_sum", "metric_args": {"column": "order_value"},
     "denominator": orders},
]
scorecard = stream.get_ab_test(diff, metrics=metrics, assignment=assignment)
```

| metric | group1_value | group2_value | delta | delta_ci_low | delta_ci_high | p_value | mde | verdict | test |
|---|---|---|---|---|---|---|---|---|---|
| conversion | 0.2433 | 0.2373 | 0.0060 | -0.0107 | 0.0228 | 0.479 | 0.0239 | inconclusive | z_test |
| adders | 0.6323 | 0.6347 | -0.0025 | -0.0213 | 0.0164 | 0.798 | 0.0270 | inconclusive | z_test |
| orders_per_user | 0.3034 | 0.2889 | 0.0145 | -0.0086 | 0.0377 | 0.219 | 0.0331 | inconclusive | welch_t_test |
| revenue_per_user | 20.14 | 18.64 | 1.50 | -0.28 | 3.28 | 0.099 | 2.55 | inconclusive | welch_t_test |
| aov | 66.39 | 64.53 | 1.86 | -1.30 | 5.03 | 0.262 | 4.64 | inconclusive | bootstrap |

The groups are the 4,960 treatment and 5,040 control users of the table (`group1_paths`, `group2_paths`), of whom 132 and 135 are silent. "Adders" counts either way of adding — `add_to_cart` or `quick_add` — because the feature replaces most treatment adds with its own event, and a metric on `add_to_cart` alone would measure the replacement, not the behaviour.

The top line is grey. The rest of this page is about reading that correctly, and about where to look next.

## Reading the scorecard

Every row is one metric (and one stratum and one period, when requested). The columns come in groups:

- **Who is compared.** `group1_paths`, `group2_paths`; `group1_paths_no_events`, `group2_paths_no_events` (silent paths, with `assignment`); `excluded_paths` (stream paths outside the table); `immature_paths` (paths left out because their observation window had not closed, with `horizon` or since-assignment periods).
- **The estimates.** `group1_value` and `group2_value` with `*_ci_low` / `*_ci_high` per arm; `delta` = group 1 − group 2 with `delta_se`, `delta_ci_low`, `delta_ci_high`; `rel_delta` = group 1 / group 2 − 1 with its own interval (NaN when group 2 is 0 or below).
- **The test.** `p_value` of the row's test; `p_value_adjusted` after the correction; `correction_family`, the text naming what was corrected together; `alpha_used`, the per-comparison level after correction.
- **What the test could see.** `mde` and `rel_mde`, the minimum detectable absolute and relative effect at `alpha_used`, the `power` you asked for (0.8 by default) and the realised sample sizes.
- **How it was computed.** `test` (`z_test`, `welch_t_test`, `bootstrap`), `ci_basis` (`wilson`, `welch`, `path_bootstrap`), `p_basis` (`pooled_z`, `welch`, `bootstrap_se`); for bootstrap rows, `valid_replicates` and `low_support` (more than 1 % of replicates without support: NaN interval, no p-value); `support_warning`, empty unless the row's support is unusual: `"one_arm_only"` (one arm has no supporting path) or `"low_support"` (a bootstrap row with fewer than about 30 supported paths per arm, where percentile intervals under-cover).

**The verdict** is a pure function of the adjusted p-value. It has five values:

- `increase` / `decrease` — `p_value_adjusted < 1 - ci_level`, with the sign of `delta`;
- `inconclusive` — everything else with a p-value, including a NaN one;
- `descriptive` — rows that compare selected groups (`period_population="active"`), with no p-value and no `mde`;
- `insufficient_data` — too little support pooled across both arms: for a 0/1 metric, fewer than `min_events` (default 10) paths with the event, or fewer than `min_events` without it, in the two arms together; for totals and values, fewer than `min_events` supported paths. The estimates stay, the test does not run. Because support is pooled, a behaviour present in one arm only — a loop the feature creates — still gets a p-value and a verdict, flagged `support_warning="one_arm_only"`.

**Read an inconclusive row with its interval and `mde`.** Conversion moved by +0.60 pp, with an interval from −1.07 to +2.28 pp. The `mde` of about 2.4 pp is the effect this test detects with 80 % power at the corrected alpha: a design quantity, not a bound on the true effect. The interval is what bounds it, and the honest summary is "an effect larger than about +2.3 pp or below about −1.1 pp is unlikely; a smaller one, in either direction, is possible" — not "the feature does nothing". See [Inconclusive is not "no effect"](#5-inconclusive-is-not-no-effect).

**The interval and the verdict can disagree, on purpose.** The interval is per comparison and never adjusted for multiplicity; the verdict reads the adjusted p-value. A row whose interval excludes 0 can still be `inconclusive` once its family is corrected — the periods section below has one. `correction="bonferroni"` adds a simultaneous interval (`delta_ci_low_adj`, `delta_ci_high_adj`) when you need one that agrees with the verdict.

**The correction family is the rows of one metric** — its strata and its periods — so `correction_family` reads `metric=conversion` on a plain scorecard, `metric=conversion/strata` with `strata_col` and `metric=conversion/periods` with `period`. Metrics are separate families and are **not** corrected against each other: with five independent metrics at 5 % each, the chance that at least one of them reads `increase` or `decrease` when nothing changed is about 23 %. Decide which metric is the decision metric before the test, and read the others as supporting evidence.

## Funnels with intervals

A funnel by arm is the first place most people look when the top line is grey. With `ci_level`, the [Funnel](/docs/widgets/funnel#confidence-intervals-and-ab-tests) puts an interval on every rate and delta and a test on every overall step:

<DemoWidget cmd={`stream.funnel(
    steps=["product_view", "cart", "checkout_start", "payment", "order_success"],
    diff=("exp_group", "treatment", "control"),
    ci_level=0.95,
    correction="holm",
)`} path="/docs-demos/guide/ab-testing/funnel-ci.html" height={640} sidebarOpen={false} />

`stream.funnel_data(...)` with the same arguments returns the numbers behind it; in percentage points:

| step | overall Δ | interval | p (Holm) | step-to-step Δ | interval |
|---|---|---|---|---|---|
| product_view | +0.25 | [−0.62, +1.12] | 1.000 | +0.25 | [−0.62, +1.12] |
| cart | −0.12 | [−2.10, +1.86] | 1.000 | −0.28 † | [−2.30, +1.75] |
| checkout_start | +1.52 | [−0.32, +3.36] | 0.527 | +2.98 † | [+0.30, +5.66] |
| payment | +0.06 | [−1.76, +1.87] | 1.000 | −4.48 † | [−6.01, −3.01] |
| order_success | +0.62 | [−1.10, +2.33] | 1.000 | +1.92 † | [−0.80, +4.64] |

Every overall step is grey. The step-to-step column (marked †, `step_conditional=True`) looks dramatic: of the users who started a checkout, 4.5 pp fewer paid in treatment. A naive two-proportion test on those two rates — what a tool that tests every number would report — gives p ≈ 5·10⁻⁹. The funnel computes no p-value for it, and never colours it.

**Why conditional steps are descriptive.** The denominator of `checkout_start → payment` is the users who started a checkout, and the treatment changed who that is: in Northwind, the treatment build also shows a "checkout now" shortcut after every add that reaches the cart, `quick_add` or not, and it starts checkouts that never pay. The step rate compares two different populations, so its difference is a mixture of the treatment's effect and the change in who got there — of unknown sign. The overall `payment` row, out of every path in each arm, is the randomised comparison, and it is +0.06 pp. The conditional rate is still worth reading: it points at the mechanism (the "checkout now" shortcut), which is a hypothesis for the next test.

Two more things the funnel's numbers assume. **The denominator is the paths with at least one event** — 4,828 and 4,905 paths here, not the 4,960 and 5,040 assigned users; `assignment=` on the funnel is not available yet, so for an ITT read of a step use `get_ab_test` with a 0/1 metric of that step and `assignment=`. **Each path is one unit**, so use the randomised grain as `path_col`.

## Periods, horizon and novelty

The scorecard above counts every event a user produced after assignment, until the data end. Users who entered in the first week were observed for eight weeks, those who entered in the sixth for two, so the effect it reports is an average over different exposure times, weighted by when users happened to arrive. Three arguments make the time dimension explicit.

**`horizon=` gives every user the same window.** Only events in `[assigned_at, assigned_at + horizon)` count, and users whose window had not closed when the data end are left out (`immature_paths`). The estimand becomes "the effect within N days of assignment":

```python
conversion = [metrics[0]]
stream.get_ab_test(diff, conversion, assignment=assignment, horizon="28D")
```

| horizon | group1_paths | group2_paths | immature_paths | delta | interval | p_value | mde | verdict |
|---|---|---|---|---|---|---|---|---|
| none | 4960 | 5040 | — | +0.60 pp | [−1.07, +2.28] | 0.479 | 2.39 pp | inconclusive |
| `"14D"` | 4960 | 5040 | 0 | +2.71 pp | [+1.34, +4.08] | 0.0001 | 1.96 pp | increase |
| `"28D"` | 4329 | 4384 | 1287 | +2.08 pp | [+0.42, +3.74] | 0.014 | 2.38 pp | increase |
| `"42D"` | 3190 | 3184 | 3626 | +0.99 pp | [−1.09, +3.07] | 0.351 | 2.98 pp | inconclusive |

These are different questions with different answers, not one question measured more or less precisely. Pick the horizon that matches the decision before the test starts — "do users buy more in their first four weeks?" — and report that one row. Trying horizons until one turns green is a form of peeking (see [pitfall 6](#6-peeking-and-open-ended-metrics)).

**`period_basis="since_assignment"` shows why they differ.** It bins each user's events by time since their own assignment, and keeps a user in a bin only when the bin's window has closed:

```python
stream.get_ab_test(diff, conversion, assignment=assignment,
                   period="14D", period_basis="since_assignment")
```

| period | group1_paths | group2_paths | immature_paths | delta | interval | p_value_adjusted | verdict |
|---|---|---|---|---|---|---|---|
| 0-13 | 4960 | 5040 | 0 | +2.71 pp | [+1.34, +4.08] | 0.0003 | increase |
| 14-27 | 4329 | 4384 | 1287 | −0.69 pp | [−1.75, +0.36] | 0.198 | inconclusive |
| 28-41 | 3190 | 3184 | 3626 | −1.52 pp | [−2.67, −0.37] | 0.019 | decrease |
| 42-55 | 0 | 0 | 10000 | — | — | — | insufficient_data |

The effect is front-loaded: positive in the first two weeks after assignment, negative from the fifth week on. The open-ended scorecard averages these, and the 14- and 28-day horizons read mostly the early part. `period_complete` is False where some assigned user's window had not closed.

**A novelty effect is a contrast between two of those windows**, and it needs an interval of its own. In the table above the windows hold different users — days 0–13 all 10,000, days 28–41 only the 6,374 whose six weeks had closed — and a difference between those rows would mix the change of the effect with the change of who is counted. `contrast=` avoids that: it compares the same paths in every contrasted bin, keeping only the paths whose windows closed in all of them, so the library balances the contrast whatever the horizon. `horizon="42D"` below makes that population explicit (with it, every bin of the table above would hold the same 6,374 users); it is a choice of window, not a requirement. The two windows' estimates are correlated, so their intervals cannot be combined by eye; [`get_ab_bootstrap()`](/docs/tools/get-ab-bootstrap) evaluates every period on the same resampled users:

```python
reps = stream.get_ab_bootstrap(
    diff, conversion, assignment=assignment,
    period="14D", period_basis="since_assignment", horizon="42D",
    contrast={"novelty": (["0-13"], ["28-41"])},
)
novelty = reps.loc[reps["contrast"] == "novelty", "delta"]
novelty.mean(), novelty.quantile([0.025, 0.975]).tolist()
# (0.0490, [0.0281, 0.0692])
```

For the same 6,374 users, the treatment's effect in days 0–13 exceeds its effect in days 28–41 by 4.9 pp, with an interval that excludes 0: a novelty effect, measured by exposure age.

**Calendar periods** (`period="W"`, the default `period_basis="calendar"`) answer a different question: did the effect hold over the calendar weeks of the test? Each week's row includes every user assigned by the end of that week, active or not (ITT per period), and the weeks of one metric are one correction family:

| week | delta | interval | p_value_adjusted | verdict |
|---|---|---|---|---|
| 2026-03-02/2026-03-08 | +1.47 pp | [−0.44, +3.38] | 0.581 | inconclusive |
| 2026-03-09/2026-03-15 | +1.71 pp | [+0.52, +2.92] | 0.040 | increase |
| 2026-03-16/2026-03-22 | +1.27 pp | [+0.33, +2.22] | 0.057 | inconclusive |
| 2026-03-23/2026-03-29 | +0.35 pp | [−0.50, +1.21] | 0.833 | inconclusive |
| 2026-03-30/2026-04-05 | −0.48 pp | [−1.25, +0.29] | 0.663 | inconclusive |
| 2026-04-06/2026-04-12 | −0.62 pp | [−1.36, +0.11] | 0.581 | inconclusive |
| 2026-04-13/2026-04-19 | −0.17 pp | [−0.87, +0.54] | 0.833 | inconclusive |
| 2026-04-20/2026-04-26 | −0.60 pp | [−1.33, +0.11] | 0.581 | inconclusive |

The third week's interval excludes 0 and its verdict is still `inconclusive`: the interval is per comparison, the verdict is corrected over eight weeks. Calendar weeks mix users of every exposure age — early weeks are full of new users, late weeks of old ones — so a trend across them looks like novelty without measuring it. `period_population="active"` restricts each week to users active in it; those rows compare survivors, whom the treatment may have changed, so they come back `descriptive`, without a p-value.

**Day-of-week.** Most products behave differently on weekends, so a period that does not contain whole weeks mixes weekdays differently from its neighbour. Use bins that are multiples of seven days (`"7D"`, `"14D"`, `"W"`), both for calendar periods and since assignment — a launch where most users entered on the same few weekdays makes "days 0–2 since assignment" a weekday-specific window.

**Before assignment.** When users have history before they were assigned, the arms should not differ on it: comparing them on the window before `assigned_at` is a free A/A check of the randomisation. `get_ab_test(assignment=...)` drops events before `assigned_at`, so this comparison is made on a separate stream of pre-assignment events. Northwind assigns users at their first app open, so it has no such window.

## Blind pre-exposure clusters as strata

A grey top line can hide users for whom the treatment works and users for whom it backfires. `strata_col=` repeats every metric within the levels of a segment — but a split is only a randomised comparison if the treatment could not have changed who is in which level. The level must be fixed **before exposure**: a signup platform, an acquisition channel, or behaviour measured on the part of each path the treatment could not yet touch. Only you know that a column such as `platform` was fixed at signup, so you declare it: `get_ab_test(..., strata_col="platform", strata_pre_exposure=True)`. Without the declaration the library reads the stream's recipe, and a stratum it cannot vouch for is flagged `exploratory=True` with a warning.

In Northwind the browsing before a user's first add is generated identically in both arms, so the window up to the first add is pre-exposure by construction. With real data it is an assumption to declare, not a fact — `quick_add` sits on catalog and search tiles, so a treated user sees it before the first add — and the least you can check is that the window does not differ between the arms (the [tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/ab_testing_tutorial.ipynb) does, in step 5). Cluster users on that window, without looking at the arm or the outcome, carry the labels onto the full stream, and stratify:

```python
pre_exposure = stream.truncate_paths(
    "path_start",
    [{"pattern": "add_to_cart", "offset": -1}, {"pattern": "quick_add", "offset": -1}, "path_end"],
)
clustered = pre_exposure.add_clusters(
    "behaviour",
    features=[{"metric": "has_event_bulk"}, {"metric": "active_days"}],
    method="kmeans",
    method_args={"n_clusters": 3, "n_init": 4, "random_state": 42},
    scaler="minmax",
)
labels = clustered.get_path_segments("behaviour")      # one label per user

# Name each cluster after its marker: the event most over-represented in it.
flags = pre_exposure.get_metrics([{"metric": "has_event_bulk"}])
flags.columns = flags.columns.str.removeprefix("has_event_bulk_")
marker = (flags.groupby(labels).mean() / flags.mean()).idxmax(axis=1)
names = marker.map(
    {"search_filter": "scanner", "collections_view": "curator", "wishlist_add": "curator"}
).fillna("other")

strata = stream.add_segment("behaviour", path_values=labels.map(names)).get_ab_test(
    diff, conversion, assignment=assignment, strata_col="behaviour",
    strata_pre_exposure=True,          # declared: the window is cut before the first add
)
```

The names come from the clusters' profiles, not from the K-means label numbers, which can change with the data or the library version: one cluster's marker is `collections_view`, tied with `wishlist_add` (both map to the dataset's name `curator`), another's is `search_filter` (`scanner`), and the third, which over-represents plain `catalog` browsing, is `other`. [Cluster Analysis](/docs/widgets/cluster-analysis) shows the same profiles interactively.

| stratum | group1_paths | group2_paths | group1_share | group2_share | delta | interval | p_value_adjusted | verdict | stratum_vs_rest_p_value |
|---|---|---|---|---|---|---|---|---|---|
| `<MISSING>` | 132 | 135 | 0.027 | 0.027 | 0.00 pp | [−2.77, +2.83] | — | insufficient_data | — |
| curator | 913 | 905 | 0.184 | 0.180 | −6.72 pp | [−9.88, −3.57] | 6·10⁻⁵ | decrease | 1.5·10⁻⁶ |
| other | 2973 | 3071 | 0.599 | 0.609 | −0.34 pp | [−2.50, +1.83] | 0.760 | inconclusive | 0.169 |
| scanner | 942 | 929 | 0.190 | 0.184 | +10.71 pp | [+6.35, +15.02] | 5·10⁻⁶ | increase | 1.6·10⁻⁷ |

How to read the strata columns:

- **`group1_share`, `group2_share`, `balance_p_value`** — each stratum's share within each arm and a chi-square test of the stratum × arm table (0.775 here). It is a share-balance check only: a stratum computed after exposure can be perfectly balanced and still selected by the treatment (below).
- **`heterogeneity_p_value`** — Cochran's Q over the strata that pass a support guard (`heterogeneity_df`, `heterogeneity_strata_used`): do the effects differ between strata at all? Here p ≈ 1.4·10⁻⁹.
- **`stratum_vs_rest_p_value`** — this stratum's delta against the delta of all the others pooled (unadjusted). Use it, not "significant here, not significant there", to say a stratum is different.
- **`strata_pre_exposure`**, **`strata_pre_exposure_source`** — whether the stratum counts as fixed before exposure, and why: `"declared"` when you passed `strata_pre_exposure=True`, `"recipe"` when the stream's recipe shows a truncation that ends at the assignment or exposure marker, `"not_pre_exposure"` otherwise (False, with a `UserWarning`). **`exploratory`** is True unless `strata_pre_exposure` is True. Neither source proves the cut sits before exposure: a declaration is your argument, and the recipe only shows where the stream was cut.
- **`<MISSING>`** holds the silent users: they produced no events, so they have no cluster.

The scanners gain about 11 pp and the curators lose about 7 pp; the `other` stratum, about 60 % of the assigned users, shows no detectable change (interval −2.50 to +1.83 pp, which still allows a small effect either way). That is why the top line is grey. Cochran's Q establishes that the effect is not the same for every user; *which* users it helps is a hypothesis — K, the features and the scaler were choices, and the largest of several stratum effects is biased upwards — to be replicated by a test targeted at those users before anyone acts on it (see [pitfall 7](#7-a-discovered-segment-is-a-hypothesis)).

**The same split after exposure tells a different story.** Story 7 of the dataset plants a sign flip by path length before the first add. Bin `length` on the pre-exposure stream, and on the full stream:

```python
bins = {"metric": {"metric": "length"}, "edges": [10], "segment_levels": ["short", "long"]}
for source, declared in [(pre_exposure, True), (stream, None)]:
    labels = source.add_segment("length_bin", metric_bins=bins).get_path_segments("length_bin")
    stream.add_segment("length_bin", path_values=labels).get_ab_test(
        diff, conversion, assignment=assignment, strata_col="length_bin",
        strata_pre_exposure=declared,  # None: read the recipe
    )
```

| length bins computed on | stratum | delta | interval | verdict | balance_p_value | strata_pre_exposure (source) | exploratory |
|---|---|---|---|---|---|---|---|
| the pre-exposure stream | short | −3.38 pp | [−5.52, −1.23] | decrease | 0.917 | True (`declared`) | False |
| | long | +6.48 pp | [+3.68, +9.27] | increase | | | |
| the full stream | short | +0.47 pp | [−0.73, +1.69] | inconclusive | 0.981 | False (`not_pre_exposure`) | True |
| | long | +0.75 pp | [−1.45, +2.95] | inconclusive | | | |

On the full stream, the bins mix pre-exposure browsing with everything the treatment caused afterwards, and the planted flip disappears; the balance p-value is 0.98 all the same. A post-exposure split can just as well manufacture a difference that is not there. Only the first table is a randomised comparison.

**New users have no history.** In an acquisition or onboarding test the treatment acts from the first screen, so there is no pre-exposure window to cluster on. Blind clusters then collapse to what is known at assignment — platform, channel, country — and clustering on post-exposure behaviour is descriptive only.

## Sample ratio mismatch

A sample ratio mismatch (SRM) — arms whose sizes differ from the design by more than chance — means the assignment, the logging or a filter lost users from one arm, and no metric of the test can be trusted until it is explained. [`get_sample_ratio()`](/docs/tools/get-sample-ratio) tests the counts against the expected shares (chi-square goodness of fit, an equal split by default):

```python
stream.get_sample_ratio("exp_group", assignment=assignment)                 # the table
stream.get_sample_ratio("exp_group")                                       # the stream
stream.get_sample_ratio("exp_group", expected_shares={"control": 0.8, "treatment": 0.2})
```

- `mismatch` is raised when `p_value < alpha` — 0.001 by default, stricter than the usual 0.05 because an SRM alarm gates every other result — **or** when any path carries more than one arm (`multi_valued_paths`), which is an assignment bug whatever the shares say.
- With `assignment=`, the counts are the table's rows, silent users included (`silent_paths`); without it, the paths of the stream. Paths with no arm or an arm outside `expected_shares` are reported in `unassigned_paths`.
- **`by=`** repeats the check within groups, with Holm-adjusted p-values over the groups in `by_levels`: `by="day"` per day of `assigned_at`, or `by="platform"` per level of a segment fixed at assignment. On Northwind all 42 assignment days pass. A mismatch confined to one day or one platform points at a deploy or a client bug that the overall test can dilute. The overall `mismatch` does not include the groups.

Run it on the assignment or on the full stream, and per group only for variables fixed at assignment. On a subset defined by behaviour — users who reached the cart — the arms' shares differ whenever the treatment works, and that difference is an effect, not a bug.

## Pitfalls

Each of these makes a path comparison look like an experiment result. The library guards against some of them; the rest are judgment.

### 1. Conditioning on post-treatment behaviour

Any split or filter by something the treatment can change — reaching a funnel step, entering a loop, path length, a cluster on the full stream — compares groups the treatment selected, with a bias of unknown sign. A balanced share between arms does not rescue it. This is the most common error in the experiment literature: Montgomery, Nyhan & Torres found it in 46.7 % of the experimental studies they reviewed in top political science journals ([AJPS](https://ajps.org/2019/06/10/conditioning-on-posttreatment-variables-can-ruin-your-experiment/)); GrowthBook calls such variables [bad controls](https://www.plushcap.com/content/growthbook/blog/growthbook-bad-controls-why-splitting-your-experiment-on-a-post-treatment-variable-backfires).

In the library: conditional funnel steps carry no p-value (`step_conditional`); strata that are neither declared (`strata_pre_exposure=True`) nor cut by a truncation in the recipe warn and are flagged `strata_pre_exposure=False`; `get_ab_test(strata_col=...)` expects a pre-exposure segment. For a loop the treatment creates, the share of assigned users who enter it (`get_ab_test` with a `matches_pattern` metric and `assignment=`) is ITT; conversion *after* the loop (`get_conversion_rate("cart->cart->cart", "order_success", diff=...)`) describes self-selected users who looped, and its result says so with `conditional=True`.

### 2. The unit of randomisation against the unit of analysis

Users were randomised, sessions are analysed: sessions of one user are correlated, so per-session intervals are too narrow and p-values too small. The fix is to analyse at the randomised grain, or to resample whole users ([Deng, Knoblich & Lu, KDD 2018](https://arxiv.org/pdf/1803.06336v2); Bakshy & Eckles, "Uncertainty in online experiments with dependent data", KDD 2013).

In the library: with `assignment=`, a `path_col` finer than the table's unit raises `InvalidParameterError`. A segment-form `diff` without an assignment table cannot be checked — the docstrings of `get_ab_test`, `funnel` and `get_conversion_rate(diff=...)` say so, and the grain is yours to choose.

### 3. Sample ratio mismatch in sub-populations

An overall split can be fine while one day, one platform or one entry point lost users; and Spotify names a filter on exposure — counting only users who "saw" the feature, where seeing it depends on the arm — as its most common cause of SRM ([Spotify, trigger analysis](https://confidence.spotify.com/blog/trigger-analysis.md); Fabijan et al., "Diagnosing sample ratio mismatch in online controlled experiments", KDD 2019).

In the library: `get_sample_ratio(by=...)` per day of assignment or per segment fixed at assignment; `multi_valued_paths` for users who carry two arms. Never read SRM on a post-exposure subset.

### 4. Multiplicity: families, FWER and FDR

Every extra metric, stratum, period, funnel step or edge is another chance of a false positive. Decide what is corrected together, and how: family-wise error control (Holm, Bonferroni) for a few confirmatory comparisons, false discovery rate control (Benjamini–Hochberg) for scans over many exploratory ones ([GrowthBook, multiple corrections](https://docs.growthbook.io/statistics/multiple-corrections); [Optimizely, Stats Engine](https://www.optimizely.com/insights/blog/statistics-for-the-internet-age-the-story-behind-optimizelys-new-stats-engine/); [Spotify, risk-aware decisions with multiple metrics](https://engineering.atspotify.com/2024/3/risk-aware-product-decisions-in-a-b-tests-with-multiple-metrics)).

In the library: the family is the rows of one metric (`correction_family`); `correction="holm"` by default, `"bonferroni"` with simultaneous intervals, `"bh"` for many exploratory rows, `"none"`; the funnel corrects across its steps. Metrics are not corrected against each other — name one decision metric in advance. A practical rule: Holm for a handful of pre-declared strata, BH for discovered clusters and scans.

### 5. Inconclusive is not "no effect"

A grey row says the test could not tell the effect from zero, not that it is zero. Compare the interval with the effect that would matter, and read `mde` to see what the test could have found. Never compute "observed power" from the observed effect — it is a restatement of the p-value. And a surprising significant result is less certain than its p-value suggests when most ideas fail: the false-positive risk depends on the prior success rate of experiments ([Kohavi, Deng & Vermeer, A/B Testing Intuition Busters, KDD 2022](https://airbnb.tech/wp-content/uploads/sites/19/2025/10/ABTestingIntuitionBusters.pdf)).

In the library: `verdict="inconclusive"` is never spelled "no effect"; `mde` is a design quantity at `alpha_used` and the realised sample sizes. An equivalence verdict ("the whole interval sits inside a margin") is not available yet.

### 6. Peeking and open-ended metrics

Re-running a fixed-horizon test every day and stopping at the first p < 0.05 inflates false positives far beyond 5 % — more than 57 % of A/A tests declared a winner in [Optimizely's simulations](https://www.optimizely.com/insights/blog/statistics-for-the-internet-age-the-story-behind-optimizelys-new-stats-engine/). With longitudinal metrics the problem has a second form: a cumulative "conversion so far" changes its meaning between looks, because each look covers different exposure times ([Spotify, the peeking problem 2.0](https://engineering.atspotify.com/2023/7/bringing-sequential-testing-to-experiments-with-longitudinal-data-part-1-the-peeking-problem-2-0)).

In the library: `horizon=` fixes the estimand ("within 28 days of assignment") and excludes users whose window is open; decide the horizon and the read date before the test, and read once. Sequential tests are not available.

### 7. A discovered segment is a hypothesis

Scanning segments for the biggest effect finds one — and the biggest of many noisy estimates is biased upwards (the winner's curse). "Significant in cluster A, not significant in cluster B" is not evidence that A and B differ. Pre-register the segments you will read, test a segment against the rest, and replicate before acting ([Statsig, differential impact detection](https://statsig.com/blog/differential-impact-detection); Spotify Confidence; Netflix's `sherlock` and [Hejazi, Zheng & Anand](https://ar5iv.labs.arxiv.org/html/2111.01223) on causal segmentation at Netflix).

In the library: `heterogeneity_p_value` (do strata differ at all) and `stratum_vs_rest_p_value` (does this one), the correction within the metric's strata, and the `exploratory` flag for strata not known to be pre-exposure. `exploratory=False` says the split counts as fixed before exposure (declared or read from the recipe), not that it was chosen before the test.

### 8. Novelty by exposure age, not calendar week

A new feature can look better (novelty) or worse (change aversion) in its first days than in the long run. The question is how the effect depends on how long a user has been exposed, and calendar weeks mix exposure ages — users who entered at launch and users who entered yesterday ([Statsig, Pulse time series](https://statsig.com/blog/using-pulse-time-series-for-deeper-insights); [Chen, Liu & Xu, LinkedIn](https://arxiv.org/pdf/1808.00114); Sadeghi et al., "Novelty and primacy: a long-term estimator for online experiments").

In the library: `period_basis="since_assignment"` bins by exposure age, and `get_ab_bootstrap(contrast=...)` gives an early-minus-late contrast a joint interval. Report the effect for the horizon that matters; do not "correct" novelty statistically. If the early days are not representative, drop them: bin since assignment and read the later bins ([Statsig, novelty effects](https://statsig.com/blog/novelty-effects)).

### Two caveats

- **New users have no pre-exposure history.** In acquisition and onboarding tests, blind clusters can only use what is known at assignment, and clusters on post-exposure behaviour are descriptive.
- **Day-of-week.** Periods that are not whole weeks mix weekdays differently; use multiples of seven days, both for calendar periods and since assignment.

## What is deferred

[ADR-0017](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0017-ab-testing-tools.md) records the decisions behind these tools and what is not in this release:

- `assignment=` on the funnel (an ITT denominator that counts silent users);
- equivalence and non-inferiority verdicts with a margin;
- a per-edge transition test between arms with Benjamini–Hochberg over edges, then dose-response;
- design from history: an A/A replay on past data to size a test and check the planned analysis's false-positive rate;
- variance reduction (CUPED, linearisation) on pre-exposure metrics;
- an exposure-based population (`exposure_event=`, `population="exposed"`) and a balance check with a within-strata permutation test;
- an `ab_test` widget with a forest plot and strata and period panels.

Sequential (always-valid) tests and a bootstrap over a unit coarser than the path are not available either: read a test once, at a horizon fixed in advance, with the randomised unit as `path_col`.

## Asking an agent instead

The [MCP server](/docs/mcp-server) exposes `get_sample_ratio` and `get_ab_test` as numbers-only tools, and its playbook has an A/B scenario: check the sample ratio first, declare the metrics up front, read `inconclusive` with the interval and the `mde`, and never filter or stratify on post-exposure behaviour. An agent with retentioneering's [skills](/docs/agent-skills) can run the same calls as code.

## Next steps

- [A/B testing tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/ab_testing_tutorial.ipynb) — a ten-step notebook on the built-in `load_ab_test()` dataset: sample ratio, scorecard, funnel, novelty, pre-exposure strata, loops, and example paths per arm.
- [A/B Test Scorecard](/docs/tools/get-ab-test), [A/B Bootstrap Replicates](/docs/tools/get-ab-bootstrap), [Sample Ratio Check](/docs/tools/get-sample-ratio) — every parameter and output column.
- [Funnel](/docs/widgets/funnel#confidence-intervals-and-ab-tests) — what each interval and p-value assumes.
- [Segments](/docs/segments) and [Add Segment](/docs/data-processors/add-segment) — carrying an assignment table or pre-exposure labels onto the event log with `path_values`.
- [Path Analysis](/docs/path-analysis) — the descriptive views that suggest what to test next.
