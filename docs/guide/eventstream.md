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

**Intervals and two-group comparisons.** `ci_level` adds a Clopper–Pearson
rate interval at the coarsest declared independent grain:

```python
rates = stream.get_conversion_rate("path_start", "payment", ci_level=0.95)
rates.attrs  # method, confidence level, observed-path population and independence
```

A valid `diff` compares disjoint groups constant at that grain. The result includes
a conservative CP-based difference and a two-sided Fisher exact p-value for
unconditional comparisons. The difference is not a Fisher-test inversion.
Conditional starts such as `add_to_cart` select who reached that event and carry
no confirmatory p-value. Their intervals remain descriptive. Method and population
metadata live in `DataFrame.attrs`; a serializer exporting only rows must preserve
those attrs separately.

On `load_ecom()`, users can switch platform across sessions, so a platform diff
at user grain can reject. Session point estimates remain descriptive; changing
the grain does not justify independent-session intervals. For an ITT outcome
including silent assignments, use the [roster scorecard](/docs/ab-testing) or
[strict fixed-window report](/docs/experiments).

### Path-level labels

`get_path_segments(segment_col)` returns one value of a segment column per path, `<MULTI>` where it changes inside the path and `<MISSING>` where it is empty throughout:

```python
stream.get_path_segments("platform").value_counts()
# desktop 229, <MULTI> 180, mobile 146, tablet 45
stream.get_path_segments("platform", path_col="session_id").value_counts()   # no <MULTI>
```

It is the inverse of [`add_segment(name, path_values=...)`](/docs/data-processors/add-segment), which broadcasts one value per path onto every event — together they carry an assignment table onto the event log, or clusters found on a truncated stream back onto the full one. `<MULTI>` paths are what the inferential methods (`get_ab_test`, `get_ab_bootstrap`, `get_conversion_rate(diff=...)`, `funnel(ci_level=...)`) refuse to split, with `DiffConfigError`; `get_sample_ratio` counts them as `multi_valued_paths` and reports a mismatch, because for a sample ratio check a path with two arms is the finding.

### Sample ratio mismatch

`get_sample_ratio` counts assignment units against the declared allocation.
Two-arm p-values are exact binomial checks under independent Bernoulli allocation.
Multiarm Pearson inference is approximate and unavailable when any expected count
is below five. Explicit expected-share keys include planned but absent arms.

```python
check = stream.get_sample_ratio(assignment=groups)  # authoritative indexed roster
check["inference_status"], check["p_value_method"], check["mismatch"]
```

Require `inference_status="ok"` before interpreting the p-value. `mismatch` is
`True`, `False`, or `None`; unavailable inference is not a pass. Conflicting
assignments independently alarm. The retained `chi2` and `dof` are descriptive
Pearson fields, not the source of a two-arm exact p-value. Fixed-total and blocked
allocation require a compatible diagnostic. `period` and `strata_col` provide
split checks with their own full planned Holm family. See
[allocation assumptions and statuses](/docs/ab-testing#sample-ratio-mismatch).

### A/B test scorecard

`ab_test_data` / `get_ab_test` compares declared independent paths. Known binary
outcomes default to Fisher exact tests with Clopper–Pearson arm intervals and a
conservative difference; nonbinary means, counts and ratios default to descriptive
estimates. Explicit nonbinary Welch/bootstrap remain approximate advanced options.
Known-binary Welch and scorecard bootstrap reject. Explicit binary z inference
is approximate and requires at least five pooled-null expected successes and
failures per arm.

```python
from retentioneering.datasets import load_ab_test, load_ab_test_assignment

stream = load_ab_test()
assignment = load_ab_test_assignment()
metrics = [
    {"name": "conversion", "metric": "has_event",
     "metric_args": {"event": "order_success"}},
    {"name": "orders_per_user", "metric": "event_count",
     "metric_args": {"event": "order_success"}},
]
scorecard = stream.get_ab_test(
    ("exp_group", "treatment", "control"), metrics,
    assignment=assignment, observation_end="2026-04-27")
scorecard[["metric", "delta", "test", "ci_basis", "verdict"]]
```

`delta` is group1 minus group2. The roster includes silent users as zero for
supported binary outcomes/totals and excludes pre-assignment events. A ratio of
sums is not an average of individual ratios. Undefined values remain missing.
The conservative difference interval is not a Fisher-test inversion; exact-path
relative intervals are withheld. Available MDE is a normal planning approximation,
not actual Fisher power or a bound on the true effect.

The default Holm family includes all declared rows and unavailable tests.
Separate calls are separate families; BH requires independence or PRDS.
`strata_pre_exposure=True` or local `pre_exposure=True` explicitly declares
pre-treatment membership. Truncation, transferred metadata and legacy inferred
recipes cannot prove it. Fisher/descriptive strata suppress asymptotic secondary
p-values; explicitly requested supported approximations label guarded diagnostics
`strata_diagnostics_basis="asymptotic_exploratory"` outside the primary family.

Calendar ITT periods include assignments before each bin end. Exposure-age bins
and fixed horizons require an explicit exclusive `observation_end`; the last
recorded event never determines maturity. Active-population periods are
descriptive. Read the full [A/B contract](/docs/ab-testing#supported-inference).

### Joint bootstrap replicates

`get_ab_bootstrap` draws whole independent units jointly across metrics and
periods. It is an explicit approximate diagnostic. The fraction of positive
draws is not a posterior probability or a p-value. Contrasts require both a
common population and common measurement eligibility across periods; changing
missingness rejects.

```python
reps = stream.get_ab_bootstrap(
    ("exp_group", "treatment", "control"), metrics[:1], assignment=assignment,
    period="14D", period_basis="since_assignment", horizon="42D",
    observation_end="2026-04-27",
    contrast={"early_minus_late": (["0-13"], ["28-41"])})
contrast = reps.loc[reps["contrast"] == "early_minus_late", "delta"]
```

Requests are capped at 1,000,000 draws and 10,000,000
replicate-by-contribution entries. Undefined/degenerate draws and insufficient
adjusted-tail resolution retain unavailable statuses. Numeric overflow raises;
more draws cannot repair heavy tails or dependence.

### Per-path table behind the scorecard

`get_ab_paths` exports one row per compared assignment with its arm, assignment
time, silent/mature flags and metric contributions. Ratios export separate
numerator and denominator columns. Aggregating those contributions over the
appropriate population reproduces scorecard point estimates.

```python
paths = stream.get_ab_paths(
    ("exp_group", "treatment", "control"), metrics, assignment=assignment)
paths.groupby("arm")["conversion"].mean()
```

Use this table for independent audits and explicitly designed analyses outside
the scorecard. See [every exported column](/docs/tools/get-ab-paths).

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

## Identity and persistent recipes

Nonmissing values in each path-ID column must use a homogeneous scalar family.
Derived nested session IDs may remain missing; that does not authorize mixing
nonmissing ID types. Mixed integer/string or
boolean/integer IDs reject before SQL can merge them. Integer object columns are
normalized exactly to signed64 or uint64; arbitrary-precision integers outside
those ranges, or a column mixing negative integers with values above signed64
maximum, reject. IDs rounded before construction cannot be recovered.

`add_segment(path_values=...)` accepts persistent labels that are strings,
booleans, finite numbers or missing. NumPy scalars normalize to native values;
timestamps/custom objects/infinite labels require explicit conversion. Mapping
keys with colliding serialized forms reject. Numeric/boolean keys retain typed
replay metadata. Recorded arguments and exported recipes are deep copies, so
mutating a caller's mapping or a returned recipe does not rewrite stream lineage.

A recipe's truncation or inherited pre-exposure flag cannot certify treatment
timing. Reassert the declaration when transferring labels; see
[A/B exposure provenance](/docs/ab-testing#segments-and-exposure-provenance).
