# Eventstream

`Eventstream` is the central object in retentioneering. It wraps your event data and exposes all widgets and data processors as methods.

> Throughout this documentation, `stream` refers to an `Eventstream` instance. All code examples assume you have created one as shown below.

## Key concepts

**Path** is the unit of analysis — an ordered sequence of events that all analysis tools (transition graph, step matrix, funnel, path metrics) operate on. What counts as a path is defined by the `path_col` you choose, not fixed by the library:

- `path_col="user_id"` — a path is the whole **user journey**. If you come from Amplitude or Mixpanel, this is the closest match to how those tools group events by user.
- `path_col="session_id"` — a path is a single **session**.

Every widget and most data processors accept a `path_col` override, so the same eventstream can be analysed at user grain and at session grain without rebuilding it — **but `path_col` must be one of the columns declared in `path_cols`** (see [schema](#schema) below); passing any other column raises an error.

`path_cols` must be listed **coarsest grain first**: every value of `path_cols[i+1]` must belong to exactly one value of `path_cols[i]` (e.g. every `session_id` belongs to exactly one `user_id`, so `path_cols=["user_id", "session_id"]` is correct). This nesting is validated against your data when the `Eventstream` is created — a schema declared the wrong way round (or a `session_id` that isn't actually unique per user) raises `SchemaConfigError` immediately, rather than producing silently-wrong analysis. See [ADR-0004](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/adr/0004-schema-and-grain-neutral-paths.md) for why.

Need to group by something that *isn't* a nested grain of your path — a device type, a campaign, an arbitrary cohort? That's not a `path_col`; use a segment column instead (see below).

**Step** is a position within a path — its 1st event, its 2nd, and so on. Steps are what [Step Matrix and Step Sankey](/docs/path-analysis) count over, and they are always relative to an anchor: the start of the path by default, or any event you choose.

**Segment** is a way to split paths into meaningful groups. A split is defined by a *segment column* that maps each event to a group: for example, a `country` column assigns one of the segment levels `US`, `DE`, `FR` to every event of a path. Segments can be static (acquisition channel, user age group, etc.) or dynamic — changing along the path, like weekend/weekday or an evolving user state (new/returning/loyal). Segment columns are declared in the [schema](#schema) (`segment_cols`) or created with the [Add Segment](/docs/data-processors/add-segment) data processor, and drive all segment-aware tools. See the [Segments](/docs/segments) page for the full story.

### Coming from Amplitude, Mixpanel or GA4

Most of the vocabulary translates, with one structural difference worth knowing up front: those tools are built around *users*, while retentioneering is built around *paths* — and a path is whatever grain you declare, so the same eventstream can be analysed per user and per session without reloading it.

| There | Here | Note |
|---|---|---|
| User / user timeline | **Path** with `path_col="user_id"` | The default. `path_col="session_id"` switches the whole analysis to session grain. |
| User property, cohort, A/B arm | **Segment column** | One segment column describes a whole *split* (`US`/`DE`/`FR`), not a single group. Values live per event, so a segment can also change along a path. |
| Event property | **Custom column** | Any extra column rides along; promote it to a segment with [`add_segment`](/docs/data-processors/add-segment) when you want to compare by it. |
| Pathfinder / Journeys / Path exploration | [Transition Graph](/docs/widgets/transition-graph) | Plus [Step Sankey](/docs/widgets/step-sankey) for the step-by-step view. |
| Funnel | [Funnel](/docs/widgets/funnel) | Ordered and unique-path-counted; see its page for the exact conventions. |
| Segment comparison | [Diff mode](/docs/widgets#diff-mode) | Every core widget renders group1 − group2 directly. |
| Session (defined server-side) | [`split_sessions`](/docs/data-processors/split-sessions) | You choose the timeout or the boundary events, and can change your mind later. |

There is no dashboard state to configure and no sampling: everything is a method call on an object you hold, computed over the full log on your machine.

## Creating an Eventstream

By default, Eventstream expects columns named `user_id`, `event`, and `timestamp`. If your data uses different column names, pass a [schema](#schema).

```python
import pandas as pd
import retentioneering as rete

df = pd.read_csv("events.csv")
stream = rete.Eventstream(df)
```

You can also pass a CSV path directly:

```python
stream = rete.Eventstream("events.csv")
```

## Expected data format

Each row in your DataFrame represents a single event. At minimum, you need a path identifier column, an event name column, and a timestamp column.

| user_id | event | timestamp |
|---|---|---|
| user_1 | page_view | 2024-01-01 10:00:00 |
| user_1 | add_to_cart | 2024-01-01 10:02:00 |
| user_1 | purchase | 2024-01-01 10:05:00 |

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame \| str` | required | Event data as a pandas DataFrame or a path to a CSV file. |
| `schema` | `dict \| None` | `None` | Schema configuration. See below. |
| `preprocess` | `bool` | `True` | When `True`, parses timestamps, casts categoricals, and sorts rows. Set to `False` if your DataFrame is already preprocessed. |

## Schema

The schema tells retentioneering which columns in your DataFrame correspond to paths, events, timestamps, and segments. Pass it as a dict to the `schema` parameter.

```python
stream = rete.Eventstream(df, schema={
    "path_cols": ["user_id"],
    "event_col": "event",
    "timestamp_col": "timestamp",
    "segment_cols": ["country", "plan"],
})
```

| Field | Default | Description |
|---|---|---|
| `path_cols` | `["user_id"]` | Columns that identify a path, ordered **coarsest grain first** (e.g. `["user_id", "session_id"]`). The first column is the primary/default path ID and every later column must nest inside all earlier ones — validated against your data at construction time. `path_col` overrides passed to tools must be one of these columns. |
| `event_col` | `"event"` | The column that contains event names — exactly one. A coarser column (the screen or category an event belongs to) is a `custom_col`; to represent the stream at that grain use [`collapse_events(group_col=...)`](/docs/data-processors/collapse-events). To *match* against such a column without changing the stream, an anchor spec takes an `event_col`, and so does a path metric's `metric_args`. The pre-5.2 `event_cols` list still works with a single element and warns. |
| `timestamp_col` | `"timestamp"` | The timestamp column. |
| `segment_cols` | `[]` | Columns treated as segmentations, available in widgets and metrics. See [Key concepts](#key-concepts). |
| `custom_cols` | `None` | Extra columns you may need for working with the eventstream. Left as `None`, every column not covered by the rest of the schema is included automatically. Set to a list — even `[]` — and only those columns (plus the ones already covered by the schema) are kept; anything else is dropped. |

## Sample dataset

retentioneering ships with a synthetic e-commerce dataset you can use to try the library without your own data. It contains six months of user sessions on a consumer electronics store, with several embedded behavioral patterns designed to showcase the analysis tools.

```python
import retentioneering as rete

stream = rete.datasets.load_ecom()
```
which is equivalent to:

```python
import retentioneering as rete

df = rete.datasets.load_ecom(as_dataframe=True)
stream = rete.Eventstream(df, schema={
    "path_cols": ["user_id", "session_id"],
    "segment_cols": [
        "platform",
        "acquisition_channel",
        "user_cohort",
        "user_lifecycle",
    ],
})
```

### A/B test datasets

For experiments, `load_ab_test(name)` generates one of six synthetic randomised tests, each with stories planted in it, and `load_ab_test_assignment(name)` returns its assignment table (the population for [`get_ab_test(assignment=...)`](/docs/tools/get-ab-test)). Nothing is downloaded: every dataset is a deterministic generator that runs in about a second, with `seed=` and `n_users=` to vary it. Some datasets hide an effect in a segment, some are broken tests whose right answer is "do not read this test" — finding out which is the exercise.

```python
import retentioneering as rete

rete.datasets.list_ab_tests()                    # name -> one-line description
stream = rete.datasets.load_ab_test("srm_platform_bug")
assignment = rete.datasets.load_ab_test_assignment("srm_platform_bug")
```

| name | size | what it practises | solutions |
|---|---|---|---|
| `northwind_home` (default) | 10,000 users, 8 weeks | the full readout: sample ratio, scorecard, funnel intervals, novelty, pre-exposure strata, loops; the [A/B tutorial](https://github.com/retentioneering/retentioneering-tools/blob/master/notebooks/ab_testing_tutorial.ipynb)'s dataset | [`ab_test.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_test.py) |
| `srm_platform_bug` | 10,000 users, 5 weeks | validity checks before any metric; `get_sample_ratio(strata_col=..., period=...)` | [`srm_platform_bug.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_cases/srm_platform_bug.py) |
| `session_assignment` | 8,000 users, 4 weeks | the unit of randomisation versus the unit of analysis | [`session_assignment.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_cases/session_assignment.py) |
| `aa_calibration` | 10,000 users, 4 weeks | what an A/A test should show; daily rows; revenue metrics | [`aa_calibration.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_cases/aa_calibration.py) |
| `trial_conversion` | 10,000 users, 6 weeks | outcomes that arrive late; `horizon` and since-assignment periods | [`trial_conversion.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_cases/trial_conversion.py) |
| `push_campaign` | 20,000 assigned users, 2 weeks | assignment from outside the product; ITT versus the users who showed up | [`push_campaign.py`](https://github.com/retentioneering/retentioneering-tools/blob/master/src/retentioneering/datasets/ab_cases/push_campaign.py) |

Each generator's docstring (`help(rete.datasets.ab_cases.push_campaign)`) describes the experiment — where the treatment sits, which segments are fixed at assignment and can be declared pre-exposure, what the assignment table is — and ends with a "Solutions (spoilers)" section: the planted stories and the correct conclusion. Every dataset has the path columns `user_id` and `session_id` and the arm segment `exp_group`; user ids do not overlap between datasets, so a table loaded for one never matches another's stream. `with_truth=True` adds the planted truth column for checking an analysis at the end.

## Inspecting your data

`stream.describe()` is a quick sanity check on what got loaded: dataset shape, schema, date range, event frequency, and path length/duration statistics.

```python
stream.describe()
```

Parameters:

- `percentiles` — percentiles (0-1) reported in `path_stats`. Default `(0.25, 0.5, 0.75, 0.9, 0.99)`.
- `top_events` — number of most frequent events to include in `event_frequency`. Default `20`; pass `None` to include every unique event, unranked and unlimited (e.g. when building a full event rename mapping, where the default cap would silently drop long-tail events).

Returns a dict:

| Key | Contents |
|---|---|
| `schema` | `event_col`, `path_col`, `path_cols`, `segment_cols`, `timestamp_col` |
| `shape` | `n_events`, `n_paths`, `n_unique_events` |
| `date_range` | `min`, `max`, `span` |
| `event_frequency` | `DataFrame` of `event`/`count`/`share`, sorted descending, limited to `top_events` rows (default 20; pass `top_events=None` for the full, unranked list). `.attrs["truncated"]` and `.attrs["n_total_events"]` say whether/how much this was cut down |
| `path_stats` | dict keyed by each entry of `path_cols`, each a `DataFrame` (`DataFrame.describe()` shape: count/mean/std/min/percentiles/max) with `length`/`duration` columns |
| `segments` | `DataFrame` of `segment_col`/`value`/`count`/`share`, one row per segment level across all segment columns |

## Other Eventstream methods

Beyond the [data processors](/docs/data-processors) and [widgets](/docs/widgets), the object exposes a handful of methods that don't fit either category.

### Getting your data back out

```python
df = stream.to_dataframe()                      # plain pandas copy
df = stream.to_dataframe(exclude_start_end=False)  # keep path_start / path_end rows
```

Processors never mutate an eventstream, so a `to_dataframe()` result is a snapshot you can hand to any other library. `stream.get_event_counts()` returns a `{event: count}` dict, and `stream.get_segment_levels()` returns `{segment_col: [levels]}` — handy before writing a `diff=` or a rename mapping.

### Per-path metrics as a feature table

`get_metrics()` runs the same [path metrics](/docs/path-metrics) registry that powers Segment Overview and clustering, and returns one row per path — a ready-to-join feature table for churn or LTV models.

```python
features = stream.get_metrics([
    {"metric": "length"},
    {"metric": "duration"},
    {"metric": "active_days"},
    {"metric": "event_count_bulk"},
    {"metric": "matches_pattern", "metric_args": {"pattern": "home->.*->purchase"}},
])
```

Metric configs here take no `agg` field — these are raw per-path values, not aggregates. `event_count_bulk` / `has_event_bulk` expand into one column per event.

`get_metric_distribution()` is the related one-off: it returns the distribution of a single metric for two segment levels against each other (`segment_levels=["a", "b"]`) or for one level against everything else (`segment_level="a"`), which is what the Segment Overview widget draws when you click a cell.

### Conversion from one event to another

`get_conversion_rate()` answers "given that Y happened, how often does X follow — and is that different from the baseline?".

```python
stream.get_conversion_rate("add_to_cart", "purchase")
```

| start_anchor | end_anchor | paths_with_start | converted | conversion_rate | base_rate | lift |
|---|---|---|---|---|---|---|
| add_to_cart | purchase | 522 | 297 | 0.569 | 0.547 | 1.041 |

Five columns instead of one number, because the number alone is not a finding:

- **`paths_with_start`** is the denominator. 0.5 out of two paths and 0.5 out of five thousand are different claims, and nothing in a rate distinguishes them.
- **`base_rate`** is the share of *all* paths where the target happens at all, and **`lift`** is the rate divided by it. In the run above, 57% of users who add to cart go on to purchase — but 55% of *all* users purchase anyway, so adding to cart tells you almost nothing at user grain (`lift` 1.04). A lift below 1 means the start event makes the outcome **less** likely, which reads far more directly than a paragraph about confounders.

`lift` is the only column you cannot derive from the others, since it needs a frequency measured over the whole eventstream rather than over the paths that reached the start.

**A window.** `within` measures from the start anchor and includes its far edge — an int counts events, a duration string counts time:

```python
stream.get_conversion_rate("product_view", "add_to_cart", within=10)   # within 10 events
stream.get_conversion_rate("support_chat", "purchase", within="30m")   # within half an hour
```

**Several targets.** A list on either side fans out into separate questions, one row per combination — a start against a set of independent outcomes, or several starts against the same one. (This is *not* `truncate_paths`' list, where several anchors describe one window bound.)

```python
stream.get_conversion_rate("add_to_cart", ["purchase", "cart", "support_chat"], within=5)
```

**Anchors, not just event names.** Both sides take the same [anchor specs](/docs/data-processors/truncate-paths) as `truncate_paths` — a `pattern`, which of its events to anchor on (`at`), which occurrence (`occurrence`), an `offset` — and `path_start` / `path_end` are ordinary names. That covers questions a pair of event names cannot ask:

```python
# exit rate: the path ended right after the error
stream.get_conversion_rate("payment_error", "path_end", within=1)

# the sessions that *landed* on the catalog, not every session that visited it
stream.get_conversion_rate(
    {"pattern": "path_start->catalog", "at": -1}, "purchase", path_col="session_id"
)
```

**The unit is the path, not the occurrence.** A path where the start event happened three times counts once in the denominator, so "of 23,000 visits to this page, how many were entrances" is a different question this method does not answer — the per-visit figure can differ from the per-path one by several points. `path_col="session_id"` is usually the right lever: at session grain "added to cart" and "purchased" describe the same visit rather than the same person, and the lift stops being washed out by everything a user ever did.

**Intervals and two-group comparisons.** `ci_level` adds a Wilson interval of the rate, and `diff` compares two groups of paths in one row:

```python
stream.get_conversion_rate("add_to_cart", "purchase", ci_level=0.95)
# ... conversion_rate 0.569, conversion_rate_ci_low 0.526, conversion_rate_ci_high 0.611 ...

stream.get_conversion_rate(
    "add_to_cart", "purchase", path_col="session_id", diff=("platform", "mobile", "desktop")
)
```

| group1_conversion_rate | group2_conversion_rate | delta | delta_ci_low | delta_ci_high | p_value |
|---|---|---|---|---|---|
| 0.240 | 0.330 | -0.090 | -0.144 | -0.034 | 0.0018 |

Every per-path column comes once per group (`group1_paths_with_start`, `group1_converted`, `group1_conversion_rate` with its interval, `group1_base_rate`, `group1_lift`, and the same for `group2_`), followed by `delta` (group 1 minus group 2) with a Newcombe interval and a pooled two-proportion `p_value` (`NaN` when it is undefined). The groups are whole paths: on `load_ecom()` 180 users switch platform between sessions, so the same call at user grain raises `DiffConfigError` instead of counting those users in both groups — compare sessions, or carry a per-path label with `add_segment(path_values=...)`. The interval treats every path as an independent unit, which is right only when `path_col` is the unit that was assigned: sessions of a user-level experiment give intervals that are too narrow. Two more things limit what this example says. `platform` is not randomised, so the delta compares two groups of sessions, it does not measure an effect of the platform. And the start anchor `add_to_cart` makes the rate conditional — the denominator is whoever reached it — so the result carries `conditional=True` (False only when the start anchor is the path start); in an experiment, a treatment that changes who reaches the anchor makes the delta and its `p_value` descriptive, and the effect on the whole arm is a job for `get_ab_test` with a `has_event` or `matches_pattern` metric and `assignment=`.

### Path-level labels

`get_path_segments(segment_col)` returns one value of a segment column per path, `<MULTI>` where it changes inside the path and `<MISSING>` where it is empty throughout:

```python
stream.get_path_segments("platform").value_counts()
# desktop 229, <MULTI> 180, mobile 146, tablet 45
stream.get_path_segments("platform", path_col="session_id").value_counts()   # no <MULTI>
```

It is the inverse of [`add_segment(name, path_values=...)`](/docs/data-processors/add-segment), which broadcasts one value per path onto every event — together they carry an assignment table onto the event log, or clusters found on a truncated stream back onto the full one. `<MULTI>` paths are what the inferential methods (`get_ab_test`, `get_ab_bootstrap`, `get_conversion_rate(diff=...)`, `funnel(ci_level=...)`) refuse to split, with `DiffConfigError`; `get_sample_ratio` counts them as `multi_valued_paths` and reports a mismatch, because for a sample ratio check a path with two arms is the finding.

### Sample ratio mismatch

Before reading any metric of an experiment, check that the arms have the sizes the design promised. `get_sample_ratio()` counts paths per arm and tests them against the expected shares (chi-square goodness of fit):

```python
check = stream.get_sample_ratio("exp_group")                 # equal split expected
check["mismatch"], check["p_value"], check["levels"]

stream.get_sample_ratio(
    "exp_group",
    expected_shares={"control": 0.8, "treatment": 0.2},
    assignment=groups,         # index user_id; columns arm and, optionally, assigned_at
    period="D",                # per assignment day, Holm-adjusted
)
```

`levels` has one row per arm (`level`, `paths`, `share`, `expected_share`, `expected_paths`). `mismatch` is raised when `p_value < alpha` (default `0.001` — an SRM alarm gates every other result, so it is set strict) **or** when any path carries more than one arm (`multi_valued_paths`), which is an assignment bug whatever the shares say. With `assignment=` the counts come from the table, including users who never produced an event (`silent_paths`), and `segment_col` is optional; without it they come from the stream. Paths left out of the test — stream paths the table does not list, paths with no arm — are counted in `unassigned_paths`. `period=` repeats the check per calendar unit of assignment (`"D"`, `"W"`, `"M"`, as in `get_ab_test`; result in `periods`) and `strata_col=` per segment fixed at assignment (`strata`), with Holm-adjusted p-values.

Run it on the assignment or on the full stream. On a subset defined by behaviour — users who reached the cart — the shares differ whenever the treatment works, and that difference is an effect, not a bug: the chi-square there is the conversion test in disguise.

### A/B test scorecard

`get_ab_test()` compares two arms of an experiment on a handful of path metrics and returns one row per metric: the value in each arm, the difference with a confidence interval, a p-value, the smallest effect the test could have detected (`mde`) and a verdict. The metrics are ordinary [path metrics](/docs/path-metrics) configs, one number per path, with an optional `name` and an optional `denominator` that turns a metric into a ratio of sums:

```python
from retentioneering.datasets import load_ab_test, load_ab_test_assignment

stream = load_ab_test()
assignment = load_ab_test_assignment()        # index user_id; columns arm, assigned_at
orders = {"metric": "event_count", "metric_args": {"event": "order_success"}}

scorecard = stream.get_ab_test(
    ("exp_group", "treatment", "control"),
    metrics=[
        {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order_success"}},
        {"name": "orders_per_user", **orders},
        {"name": "revenue_per_user", "metric": "column_sum", "metric_args": {"column": "order_value"}},
        {"name": "aov", "metric": "column_sum", "metric_args": {"column": "order_value"},
         "denominator": orders},
    ],
    assignment=assignment,
)
```

| metric | group1_value | group2_value | delta | delta_ci_low | delta_ci_high | p_value | mde | verdict | test |
|---|---|---|---|---|---|---|---|---|---|
| conversion | 0.2433 | 0.2373 | 0.0060 | -0.0107 | 0.0228 | 0.479 | 0.0239 | inconclusive | z_test |
| orders_per_user | 0.3034 | 0.2889 | 0.0145 | -0.0086 | 0.0377 | 0.219 | 0.0331 | inconclusive | welch_t_test |
| revenue_per_user | 20.14 | 18.64 | 1.50 | -0.28 | 3.28 | 0.099 | 2.55 | inconclusive | welch_t_test |
| aov | 66.39 | 64.53 | 1.86 | -1.30 | 5.03 | 0.262 | 4.64 | inconclusive | bootstrap |

`delta` is group 1 minus group 2. The test follows the metric: a 0/1 metric gets a pooled two-proportion z-test with Wilson and Newcombe intervals, a per-path total or value gets Welch's t-test, a ratio of sums (average order value = revenue / orders) gets a joint path bootstrap — paths are resampled within each arm and every metric is recomputed on the same resample, so the ratio's interval respects that revenue and orders come from the same users. `test=` (or a per-metric `"test"`) overrides the choice where it fits the metric.

**Read an inconclusive row with its interval and `mde`.** Above, conversion moved by +0.60 pp with an interval from -1.07 to +2.28 pp: an effect larger than about +2.3 pp or below about -1.1 pp is unlikely, and a smaller one, in either direction, is possible. The `mde` (about 2.4 pp) is the effect the test detects with 80 % power at alpha 0.05 (`mde_alpha`) — a design quantity, not a bound on the true effect: an inconclusive result does not say the effect is smaller than the `mde`, the interval says how large it can be. The verdict is `increase` / `decrease` only when the multiplicity-adjusted p-value is below `1 - ci_level`; `insufficient_data` marks rows whose support, pooled across both arms, is below `min_support` (default 10; for totals, paths with a non-zero total), with `support_warning="no_data"` when no path of either arm has the behaviour; a behaviour present in one arm only still gets a p-value, flagged `support_warning="one_arm_only"`.

**The population.** `assignment=` makes the assignment table the population (intention to treat): users who were assigned but never produced an event count as zeros (`group1_silent_paths`), events before `assigned_at` are dropped, and stream paths missing from the table are counted in `unassigned_paths`. Without it the population is the paths of the stream; the `population` column of every row says which (`"assignment"` or `"stream"`). Either way the unit is the path: with a user-level assignment table, or an arm segment constant within the users, a session `path_col` raises, because per-session intervals of a per-user experiment are too narrow.

**Strata.** `strata_col=` repeats every metric per level of a segment that is constant per path, with each stratum's share in both arms, a share-balance p-value, Cochran's heterogeneity test and a stratum-versus-rest test. The p-values are corrected within each metric (`correction="holm"` by default; `"bonferroni"` also adds simultaneous intervals, `"bh"` controls the false discovery rate; `correction_scope="all"` corrects every row of the call together instead, for a scorecard read without one decision metric), and `correction_family` says what was corrected together. A stratum must be fixed before exposure: find it on a stream truncated before the first exposure and carry it over with [`add_segment(path_values=..., pre_exposure=True)`](/docs/data-processors/add-segment), which records your declaration with the segment. A source column fixed at assignment, such as a signup platform, is declared with `strata_pre_exposure=True`. A stratum that is neither declared nor read from a truncation in the stream's recipe — clusters or `metric_bins` on the full stream — raises a warning and is flagged `strata_pre_exposure=False` — the treatment can move users between such strata, so their rows compare selected groups, not randomised ones. `strata_pre_exposure_source` says which case applies (`"declared"`, `"recipe"`, `"not_pre_exposure"`), and rows are `exploratory=True` unless the stratum counts as pre-exposure.

**Periods and windows.** `period="W"` gives one row per calendar week, each on every user assigned by the end of that week (ITT per period). `period_population="active"` restricts each week to users active in it; such rows compare survivors and are marked `descriptive`, with no p-value. `period_basis="since_assignment"` bins events by time since each user's assignment (`"0-13"`, `"14-27"` days for `period="14D"`), and keeps a user in a bin only when the bin's window closed by the stream's last event (`immature_paths` counts the others). `horizon="28D"` gives every user the same 28-day window from assignment. Different horizons answer different questions — choose the one that matches the decision before reading any.

### Joint bootstrap replicates

`get_ab_bootstrap()` returns the replicates behind the bootstrap: one row per replicate, metric, stratum and period, with both arms' values and the delta, all computed on the same resampled users. Combine them freely — an effect that changes with exposure age is the difference between an early and a late window, with an interval that accounts for the same users being in both. `contrast=` compares the same paths in every contrasted bin — the library keeps only the paths whose windows closed in all of them — so the horizon below is a choice of window that makes the population explicit, not a requirement:

```python
reps = stream.get_ab_bootstrap(
    ("exp_group", "treatment", "control"),
    metrics=[{"name": "conversion", "metric": "has_event",
              "metric_args": {"event": "order_success"}}],
    assignment=assignment,
    period="14D",
    period_basis="since_assignment",
    horizon="42D",
    contrast={"novelty": (["0-13"], ["28-41"])},
)
novelty = reps.loc[reps["contrast"] == "novelty", "delta"]
novelty.quantile([0.025, 0.975]).tolist()
# [0.0284, 0.0675]
```

The interval comes from the replicates; the estimate is the same difference on the observed data, +4.86 pp: the effect is larger in days 0-13 than in days 28-41 (`get_ab_test(..., period="14D", period_basis="since_assignment", horizon="42D")` gives +3.38 pp and −1.48 pp for the two bins, and MCP's `get_ab_bootstrap` reports this `delta`).

### Per-path table behind the scorecard

`get_ab_paths()` takes the `diff`, `metrics`, `assignment`, `horizon` and `path_col` of `get_ab_test()` and returns the table its rows are computed from: one row per path of both arms, with the arm, `assigned_at` when the table has it, `silent`, `mature` (False for a path whose `horizon` window had not closed) and every metric's per-path value (a ratio metric as its numerator and denominator columns). Its per-arm means reproduce the scorecard's `group1_value` and `group2_value`. Use it for an analysis the scorecard does not run, such as a covariate adjustment (CUPED) fitted by hand:

```python
paths = stream.get_ab_paths(
    ("exp_group", "treatment", "control"),
    metrics=[{"name": "conversion", "metric": "has_event",
              "metric_args": {"event": "order_success"}}],
    assignment=assignment,
)
paths.groupby("arm")["conversion"].mean()     # the scorecard's group values
```

See [A/B Per-Path Table](/docs/tools/get-ab-paths) for every column.

### Which edges and step cells differ between arms

`get_ab_transitions()` takes the `diff`, `assignment`, `horizon` and `path_col` of `get_ab_test()` and tests every edge of the transition graph between the arms: one row per edge with each arm's value of `edge_weight`, the delta and its interval, a p-value adjusted over the tested edges (Benjamini–Hochberg by default) and the scorecard's `verdict` and `reading`. `unique_paths` (the share of paths with the transition) and `avg_per_path` are tested with the z-test and Welch's test; `proba_out`, `proba_in` and `share_of_total` with the joint path bootstrap, and they are `conditional=True`, because their denominators are behaviour the treatment changes. `get_ab_steps()` does the same for the step matrix, one row per `(event, step)` cell. On the stream's paths an arm's value equals what `transition_graph_data()` / `step_matrix_data()` show for it.

```python
edges = stream.get_ab_transitions(("exp_group", "treatment", "control"), assignment=assignment)
edges.attrs["n_tested"], edges["verdict"].value_counts().to_dict()
# (90, {'inconclusive': 71, 'increase': 10, 'decrease': 9, 'insufficient_data': 2})
stream.transition_graph(diff=("exp_group", "treatment", "control"), ab_transitions=edges)
```

The family is every edge with enough support, found by looking, so these are screens: a discovery is a hypothesis for a declared metric of a new test, read after the scorecard. See [A/B Transition Screen](/docs/tools/get-ab-transitions), [A/B Step Screen](/docs/tools/get-ab-steps) and the [A/B Testing guide](/docs/ab-testing#which-edges-and-steps-differ-a-screen-with-fdr-control).

The widgets [`ab_transitions()`](/docs/widgets/ab-transitions) and [`ab_steps()`](/docs/widgets/ab-steps) draw the same frames as difference matrices (one cell per row, its background the delta, a wide outline for its adjusted p-value), and [`get_ab_transitions_table()`](/docs/tools/get-ab-transitions-table) / [`get_ab_steps_table()`](/docs/tools/get-ab-steps-table) return them in the shape of the diff matrix, a group of statistic columns per target (step), for `to_csv` and `to_excel`.

### Reproducing an eventstream

Every processor call is recorded, so a derived eventstream can describe how it was built:

```python
prepared = stream.filter_events(drop={"event": ["checkout_bug"]}).truncate_paths(
    start_anchor="path_start", end_anchor="purchase"
)
prepared.recipe()
# [{"type": "filter_events", "drop": {...}},
#  {"type": "truncate_paths", "start_anchor": "path_start", "end_anchor": "purchase"}]
```

`Eventstream.from_recipe(df, recipe)` replays that list onto a base DataFrame, rebuilding an identical eventstream — useful for moving a prepared pipeline between notebooks, storing it next to a report, or handing it to the [MCP server](/docs/mcp-server), whose preprocessor steps use exactly this format. `stream.fingerprint` (a property) is a content hash for checking two eventstreams really are the same, and `stream.equals(other)` compares them directly.
