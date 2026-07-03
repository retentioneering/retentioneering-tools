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
     "values": [
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

**Values format:** each entry is `[column, op, value, label]`; the last entry `["anomaly"]` is the ELSE fallback.

**Concrete example** — spike on Jan 20–22 2024, timestamp column is `timestamp`:
```json
{
  "type": "add_segment",
  "name": "period",
  "values": [
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

   Segment levels are named after the **last event reached** in sequence:
   - `out_of_funnel` — never reached A
   - `A` — reached A but not B
   - `B` — completed the funnel

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


## noise_removal

**Scenario:** Clean data before analysis

**Trigger:** Data has very short sessions, irrelevant events, or repetitive self-loops.

**Steps:**

Confirm with the user, then call `update_base_stream` with one or more of:
```
{"type": "collapse_events", "repetitive": true}        — removes A→A→A loops
{"type": "filter_paths", "op": ">", "metric": "length", "value": 3}
{"type": "filter_events", "column": "event", "values": ["noise_event"], "exclude": true}
```
