# Export events with observation context

Export an analysis period together with bounded history before and after it, then retain that context as events or compact session features. This prevents an export boundary from silently becoming a user's first visit or evidence of retention.

## Define the contract before querying

Use UTC instants at microsecond precision and half-open intervals:

| Interval | Meaning |
|---|---|
| `[A, B)` | Main analysis period; an event at A belongs, an event at B does not. |
| `[L, A)` | Additional history on the left. |
| `[B, R)` | Follow-up on the right. |
| `[L, R)` | Certified continuous observation, also the scan interval in these templates. |

Choose requested lookback/lookahead, then intersect with your **verified** coverage:
`L = max(A - lookback, coverage_start)` and `R = min(B + lookahead, coverage_end)`.
Require `L <= A < B <= R`. Record requested bounds separately so truncated history is visible.
The coverage end is an ingestion watermark, not `MAX(event_timestamp)` and not today's date.
An empty day can be fully observed; a day with events can still be incomplete.

The templates assume the same continuous coverage for every selected user, stable identity,
and all relevant activity types. Validate those assumptions from ingestion manifests,
tracking availability and identity policy. Neither SQL nor a session gap can establish them.
If coverage differs by user, join a coverage table, carry individual bounds, and restrict each
user's events and outcome evaluation accordingly. If there are outages inside the interval,
split into continuous observation episodes or mark affected outcomes `unknown_coverage`;
do not bridge an outage with the inactivity rule. A known return proves activity but an
outage prevents calling it the *first* return. Consent loss is not inactivity.

These queries select users with at least one core event. They do **not** estimate retention
of all registered users: users with no core activity require an external cohort/registration
table and a left join. Keep those users in a separate user-level outcome table even when
there are no events to put into Eventstream.

Convert local calendar boundaries to UTC **independently**, using an IANA timezone. A local
day may last 23 or 25 hours. Resolve ambiguous/nonexistent civil times explicitly upstream;
never localize already-UTC event timestamps again. Timeout `30m` and outcome horizon `14d`
here mean elapsed 1,800 and 1,209,600 seconds, not calendar boundaries.

## Run the SQL templates

The files are runnable templates after replacing table names and parameters:

- [BigQuery GoogleSQL](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/sql/bigquery_observation_window.sql)
- [ClickHouse SQL](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/sql/clickhouse_observation_window.sql)
- [GA4 normalization adapter](https://github.com/retentioneering/retentioneering-tools/blob/master/docs/sql/bigquery_ga4_normalize.sql)

Both main queries accept a normalized table:

| Column | BigQuery | ClickHouse |
|---|---|---|
| `user_id`, `event`, `event_id` | `STRING`, non-null | `String` |
| `timestamp` | `TIMESTAMP`, non-null | `DateTime64(6, 'UTC')` |

`event_id` must be unique within user and supply a deterministic tie order. Preserve a
source sequence when available; a deterministic arbitrary tie-breaker does not establish
causal order. Resolve ingestion duplicates upstream. Do not deduplicate simultaneous
legitimate events simply because names and timestamps coincide.

Edit the `params` CTE: the example analyzes September 2026, observes August through
October 15 inclusive, uses a 30-minute session timeout and a 14-day outcome horizon.
The `inactive_14d` name must change if you change that horizon. BigQuery uses
`UNIX_MICROS(TIMESTAMP '...+00')`; ClickHouse uses
`toUnixTimestamp64Micro(toDateTime64('...', 6, 'UTC'))` to avoid rounding gap comparisons.

For a generic BigQuery table, normalize with
`SELECT user_id, event, event_id, timestamp FROM your_table` (convert numeric epoch values
with `TIMESTAMP_MICROS`, not an implicit cast). For ClickHouse, create the normalized table
with the types above and load deduplicated rows, or replace `source` with a view over your
existing schema. On a versioned `ReplacingMergeTree`, resolve versions before sessionization.

For GA4, run the adapter first. It uses `user_pseudo_id` as the device/browser identity,
not a cross-device person. It keeps identical rows; its generated ID establishes an export
order only. Decide whether/how to stitch identities before running the recipe. It intentionally
recomputes inactivity sessions rather than claiming equivalence with GA4 sessions.
If importing GA4's `ga_session_id`, combine it with user identity and handle missing IDs;
the numeric session ID alone is not globally unique.

GA4 daily suffixes follow the property timezone while event timestamps are UTC microseconds.
Choose suffix bounds covering every local date touched by `[L,R)`; the adapter's broad final
suffix is intentional. The main query still applies exact timestamp bounds. Use finalized
daily tables, exclude `events_intraday_*`, and freeze a snapshot after late arrivals have
settled. Missing partitions need an ingestion audit, not an assumption of zero activity.
Do not union daily and intraday blindly. See the official
[GA4 export schema](https://support.google.com/analytics/answer/7029846) and
[BigQuery timestamp functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions).

For large datasets, materialize the normalized extended interval once and restrict to core
users before window sorting; apply partition pruning **in addition to**, not instead of,
the exact timestamp predicate. CTEs are not a guarantee of materialization. Inspect the query
plan and dry-run BigQuery for bytes scanned before executing against production data.

## What the compact export preserves

The query orders each user's entire `[L,R)` history and starts a new session only when
`current_timestamp - previous_timestamp > timeout`. Equality stays in the same session,
as in `split_sessions(timeout="30m")`. It computes sessions before filtering to `[A,B)`.
ClickHouse uses nullable `lagInFrame`/`leadInFrame` with an explicit full frame so a missing
neighbor stays NULL rather than becoming an epoch date; see its
[window-function documentation](https://clickhouse.com/docs/sql-reference/window-functions/lagInFrame).

Each exported row is a **real core event**, with these additional fields:

| Field | Interpretation |
|---|---|
| `observed_session_index` | Session ordinal within this scan, never a lifetime ordinal. |
| `session_start_us`, `session_end_us`, `session_event_count` | Bounds and count using all observed events in this session, including outside the core. |
| `previous_session_end_us`, `next_session_start_us` | Adjacent distinct sessions in the observed history; they may be inside or outside the core. |
| `previous_event_us`, `next_event_us` | Last event before A / first event at or after B for this user, possibly in a boundary-crossing session. |
| `session_left_censored`, `session_right_censored` | The scan cannot establish the session's full extent on that side. |
| `observed_start_us`, `observed_end_us` | Certified coverage, retained even when no neighboring event exists. |
| `anchor_us`, `outcome` | Last core event and fixed-horizon follow-up label defined below. |

A NULL previous event means **no observed prior event in `[L,A)`**, not a new user. A finite
lookback can establish a return after a specified inactivity period; it cannot prove a
first-ever session. That needs complete history since registration/birth plus verified
identity continuity. Left censoring of lifetime history and left censoring of a particular
session are different: a session can have a known start while the user has older unseen sessions.

A boundary session can extend arbitrarily far through a chain of short gaps; one timeout
of padding does not guarantee its full duration. The flags remain true until a separating
gap is observed or coverage extends far enough. On the left, `session_start - L >= timeout`
is sufficient because the predecessor would be strictly before L. On the right,
`R > session_end + timeout` is required because R is excluded. A complete session's duration
is `(session_end_us - session_start_us) / 1e6`; it measures first-to-last event, not the
additional idle timeout. Censored durations are lower bounds.

To export all contextual events, remove only the final `WHERE is_core`. Keep `is_core` and
the coverage metadata. To minimize transfer further, save core events, one distinct row
per `(user_id, observed_session_index)` for session features, and one row per user for
coverage/outcomes as separate tables. Compact features preserve the stated summaries,
not the entire external path: you cannot reconstruct its transitions or event mix from them.
Do not invent `previous_session`/`next_session` events; they would distort paths and metrics.

## Use the export with Eventstream

Read either query result as `export_df` (prefer Parquet to preserve nullable integers and
microseconds). The following block is also exercised by the documentation tests.

```python
import json
import pandas as pd
from retentioneering import Eventstream

# SQL already assigned sessions using the extended history.
export_df = export_df.copy()
export_df["timestamp"] = pd.to_datetime(export_df["timestamp"], utc=True)
# Use UTC-naive consistently for Eventstream/DuckDB comparisons in this example.
export_df["timestamp"] = export_df["timestamp"].dt.tz_localize(None)
export_df["session_id"] = [
    json.dumps([str(user), int(index)])
    for user, index in zip(export_df["user_id"], export_df["observed_session_index"])
]
# An explicit tie order survives Eventstream's sorting.
export_df = export_df.sort_values(["user_id", "timestamp", "event_id"])
export_df["subindex"] = export_df.groupby("user_id").cumcount()
stream = Eventstream(export_df, schema={
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["outcome"],
})
core = stream.filter_events(sql="SELECT * FROM eventstream WHERE is_core")
# Core metrics describe the clipped path, not lifetime or full session duration.
core_metrics = core.get_metrics([{"metric": "length"}, {"metric": "duration"}])
```

Do not call `split_sessions` on the compact export: that would discard the boundary context
and renumber sessions. For a full export without SQL sessionization, call it on the extended
stream **before** filtering core events, and retain coverage/censoring metadata separately.
For full-session duration use the summary columns above (once per session), not the
`duration` metric over clipped events. For whole-user duration, neither core nor bounded
extended duration establishes a lifetime: report observed span or use a censoring-aware estimator.

## Follow-up outcomes: return, inactivity, or censoring

For each core-active user, let `t` be their last core event and `d = t + 14 elapsed days`.
The question is: **was there another activity event in `(t,d]`?** It is not permanent churn.

| Evidence | `outcome` |
|---|---|
| Next observed event at or before d | `returned` |
| No event in `(t,d]`, continuous coverage through d (`R > d`) | `inactive_14d` |
| Otherwise | `right_censored` |

An event exactly at d counts as a return; one at R is not in the extract. A return after d
can coexist with `inactive_14d`. A return proves this binary outcome even if later follow-up
is short. For non-returners, never substitute the dataset's last event for a coverage watermark.
A NULL next event can mean either observed inactivity or insufficient follow-up; the bounds
are what distinguish them. Report all three counts and the censored fraction. Excluding
censored users changes the estimand and may bias comparison if coverage is informative;
use survival methods when varying follow-up is central to the question.

The templates treat every exported event as activity. If your churn definition counts only
purchases or other qualifying events, compute the anchor and next qualifying event from that
subset, preserving the unfiltered behavioral stream and its coverage; users with no qualifying
anchor need a separate `not_eligible` state. Do not remove events used for sessionization merely
to change the churn definition.

For prediction, features must stop at a declared prediction time. Right-context columns and
`outcome` are labels/validation metadata, not model inputs. Temporal precedence alone does
not establish causality. The [churn recipe](/docs/recipes#find-what-leads-to-churn) uses the
fixed end-of-period definition here; an onboarding study instead needs a verified signup
anchor, a fixed feature window, and a later outcome window.

## Validation before interpreting results

1. Assert `L <= A < B <= R`, positive timeout/horizon, and certified continuous coverage.
   Reject unknown coverage rather than silently manufacturing inactivity labels.
2. Reject NULL identifiers/timestamps/names and duplicate `(user_id,event_id)` keys.
   Audit excluded anonymous events and record the identity/deduplication policy.
3. Core event keys and counts must match the direct `[A,B)` extract exactly; joins must not
   multiply rows. Full and compact exports must agree on core session IDs and summaries.
4. Exercise events at L, A, B, R; timeout equality and ±1 microsecond; horizon equality;
   tied timestamps; empty follow-up; a return after the horizon; users only outside the core.
5. Include a session crossing A/B and one clipped at L/R. A no-history user must not become
   `first_ever`; a missing neighbor must stay NULL, especially in ClickHouse.
6. Check local DST transitions by converting both calendar endpoints to UTC, and ensure
   database/session display timezone does not change membership or labels.
7. Save parameters, coverage evidence, snapshot/version, query revision, event counts and
   censored counts with the analysis. Rerun after late arrivals or identity corrections.

Run `uv run pytest tests/docs/observation_windows_test.py -q` for the executable Eventstream
and censoring examples. Run the optional SQL checks with:

```bash
uv run --with chdb --with sqlglot pytest tests/docs/observation_windows_test.py -q
```

The latter executes ClickHouse SQL with chDB and checks BigQuery semantics by transpiling
GoogleSQL to DuckDB with SQLGlot. This is **not** a BigQuery service validation. Test your
source adapter and run BigQuery's dry run/execution on a small warehouse fixture before
using a production export. The local SQL fixture replaces the normalized source CTE;
it does not certify your warehouse schema, partition selection or ingestion coverage.
