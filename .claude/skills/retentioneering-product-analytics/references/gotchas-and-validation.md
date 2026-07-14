# Gotchas and validation

Two lists with different natures. **API gotchas**: current library behaviors that
surprise; work around them as described. **Analysis integrity**: methodological traps
that produced wrong-but-plausible conclusions in real investigations until a reviewer
caught them; validate against each before shipping numbers.

## A. API gotchas (version-verified)

| # | Behavior | Handle it |
|---|---|---|
| G1 | `truncate_paths` DROPS paths missing either anchor; there is no keep-whole option | for "truncate converters, keep the rest whole": `filter_paths` split → truncate the converter half → analyze separately (concatenating streams is manual) |
| G2 | No order-only mode: `timestamp_col` is mandatory | for logs with click order but no clock: synthesize `base_date + order * 1s`; then durations/`time_median` are MEANINGLESS — never report them, only step counts |
| G3 | `time_between` = first A to first B **globally**, not first-B-after-A | off-by-few on paths where B precedes A; compute strict semantics via `to_dataframe()` when it matters |
| G4 | `funnel` is closed/ordered only | for presence-based ("did all of these happen, any order") use `has_all_events` via `get_metrics` |
| G5 | Metric arg keys: single event → `event`, lists → `events` (only on `has_any_event`/`has_all_events`) | copy from api-map, not from memory; errors are loud and name the requirement |
| G6 | Cluster result may LACK `best_params` when silhouette is undefined for every candidate (degenerate/duplicate feature rows) | guard `res.get("best_params")`; treat as "no cluster structure" |
| G7 | Cluster labels are strings `"cluster_0"...`; diff values must match segment levels exactly | check `get_segment_levels()` before writing `diff=(...)` |
| G8 | Transition matrix may be integer-typed with NaN gaps | before matrix math: `.astype(float).fillna(0)` |
| G9 | Graph widgets with >~100 nodes open as a dense hairball | teach the edge-weight threshold in the sidebar; or pre-aggregate events into families (`rename_events`) |
| G10 | `stream.df` is read-only | mutate via processors; materialize with `to_dataframe()` |

Fixed in 5.0 — do NOT carry these legacy workarounds forward: identifier quoting for
SQL-reserved column names; nondeterministic `matches_pattern` ordering; `n_clusters`
range strings; silent `describe()` truncation (now flagged in `.attrs`, disable with
`top_events=None`); silent empty widget exports (now `WidgetExportError`); substring
pattern matching; anchor off-by-one in step matrix; `diff` sign (now v1−v2).

## B. Analysis integrity checklist

Run before presenting conclusions. Each item traces to a real wrong-conclusion incident.

1. **Denominators.** Every percentage names its base. `funnel_data.conversion_rate` is
   share of ALL paths; step-to-step is `step_conversion_rate`. *(Incident: "2% conversion"
   nearly shipped where the true step rate was 32%.)*
2. **Population.** Who exactly is in the analysis? Watch entry filters (`truncate_paths`
   drops non-completers, G1) and "engaged users only" thresholds. *(Incident: a ≥10-min
   activity cutoff silently excluded 43% — the weakest users, the ones the decision was
   about; on the full cohort the verdict got harsher, not softer.)*
3. **Survivorship.** Late-stage aggregates describe survivors. Report "of all who
   started / of those who reached" side by side. Ratings/feedback exist only for
   finishers.
4. **Exposure confound.** Longer paths contain more of everything (loops, feature
   touches) AND convert more. Any "users who did X convert better" claim needs
   stratification by path length or a matched design.
5. **Target leakage.** No outcome events in clustering features (R5) or in
   sequence/pattern features computed over the full path — cut paths at the first
   outcome before mining predictive patterns.
6. **Structural vs behavioral lift.** If the flow FORCES step A before outcome B, "users
   who did A convert ×100" is architecture, not behavior. Say which one you measured.
7. **Small cells.** Shares without `n` are noise bait; flag cells with n below ~30 and
   add intervals (Wilson for proportions) when a decision hangs on them.
8. **Right/left censoring.** Log windows cut both ends: "never returned" near the window
   edge and "first session" of users active before the window are both suspect.
9. **Correlation discipline.** Observational path data supports "associated with", not
   "drives", unless there is an experiment. End reports with hypotheses + suggested A/B,
   not causal claims.
10. **Numbers ↔ visuals.** Every caption number comes from the headless twin of the
    widget shown; write `analysis=` texts after conclusions are frozen.

## C. Self-checks worth automating

```python
# conservation: filters explain themselves
before = stream.describe()["shape"]; after = filtered.describe()["shape"]
# funnel sanity: step counts are non-increasing
# diff sanity: diff block equals g1 - g2 on a sample cell
# determinism: rerun the pipeline; assert identical key outputs
# reproducibility: Eventstream.from_recipe(raw_df, processed.recipe()) equals processed
```
