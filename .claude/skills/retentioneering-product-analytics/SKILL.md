---
name: retentioneering-product-analytics
description: >
  Analyze event logs, clickstreams, user paths, product funnels, retention,
  behavioral segments, transition graphs, step matrices, sequence patterns,
  and customer journeys using Retentioneering. Use when the user provides
  CSV, Parquet, pandas, or database event data containing user, event, and
  timestamp columns, or asks why users convert, churn, loop, abandon a flow,
  or follow particular product paths. Do not use for qualitative
  journey-mapping workshops or aggregate website traffic without user-level
  event sequences.
license: Apache-2.0
compatibility: >
  Requires Python >= 3.11 and Retentioneering 5.x. Designed for local CSV,
  Parquet, and pandas event logs. Network access is not required for local
  analysis.
metadata:
  author: retentioneering
  version: "1.0.0"
  package: retentioneering
  category: data-analysis
  keywords: >-
    clickstream, event log, user paths, customer journey, product analytics,
    funnel analysis, retention, churn, behavioral segmentation, transition
    graph, step matrix, sankey, sequence mining, markov chain, pandas, duckdb
  homepage: https://retentioneering.com
  documentation: https://retentioneering.com/docs
  repository: https://github.com/retentioneering/retentioneering-tools
---

# Retentioneering product analytics

## Objective

Turn event-level behavioral data into a reproducible answer to a **product question** —
why users convert, churn, loop, or abandon — using user trajectories, transitions,
funnels, and behavioral segments.

Do not merely generate visualizations. Connect each output to the question, separate
observation from interpretation, and never present path correlations as causal effects.

## Bundled references (read on demand, not upfront)

| File | Read it when |
|---|---|
| `references/api-map.md` | before writing any Retentioneering call — verified signatures, argument conventions, return shapes for 5.x |
| `references/analysis-recipes.md` | after the question is clear — 10 field-tested patterns (R1–R10) with skeletons and pitfalls |
| `references/gotchas-and-validation.md` | before executing (API gotchas G1–G10) and before presenting (integrity checklist B1–B10) |
| `scripts/inspect_event_log.py` | step 2 — automated data profiling and schema suggestion |

## Required event-log semantics

Minimum: a path identifier (user or session), an event name, a timestamp (or a reliable
order column — see gotcha G2 for order-only data). Useful extras: session id, segment
attributes (device, source, plan), event properties, conversion labels.

## Workflow

### 1. Environment

1. Confirm the package: `python -c "import retentioneering; print(retentioneering.__version__)"`.
   Expect 5.x; this skill's API map is version-verified for 5.0 — on a different major
   version, trust installed docstrings over the map.
2. Locate the event data (CSV / Parquet / frames in existing code). Never modify inputs.
3. Do not invent methods: anything not in `references/api-map.md` must be verified
   against the installed package before use.

### 2. Inspect the data BEFORE choosing methods

Run `scripts/inspect_event_log.py <path> [--sep ...]` (or replicate its checks inline for
in-memory frames). It profiles columns, infers the user/event/timestamp mapping, checks
timestamp parseability, duplicates, per-path ordering, path-length distribution, and
emits `artifacts/data-profile.json` plus a ready-to-paste `Eventstream(...)` schema.

Report to the user before proceeding: inferred mapping, row/user/event-type counts,
covered period, and any red flags (nulls in key columns, timestamp ties, suspected bots
or ultra-long paths, order-only timestamps). Confirm the mapping if inference is
ambiguous.

### 3. Frame the product question, then pick the SMALLEST recipe

Map the question to a recipe in `references/analysis-recipes.md`:
navigation structure/loops → transition graph (R1/R6) · before/after an anchor →
step matrix (R3) · ordered conversion flow → funnel (R1/R2) · what winners do
differently → diff on a funnel-stage segment (R2) · heterogeneous users → clustering
without target leakage (R5) · between two funnel levels → truncate micro-journey (R4) ·
intervention timing → time-to-outcome (R7) · cross-segment scan → segment overview (R8) ·
value of a fix → Markov what-if (R9, advanced).

Combine recipes only when each addition resolves a distinct uncertainty.

### 4. Execute reproducibly

1. Prefer a rerunnable script (or a notebook executed top-to-bottom) over ad-hoc cells.
2. Write artifacts to a dedicated output directory (`artifacts/` by default).
3. Log every filtering rule and its row/path impact; never silently drop data
   (integrity item B2).
4. Use `sample_paths(frac=, random_state=)` for stable subsamples; stochastic steps get
   explicit seeds.
5. Record lineage: save `processed.recipe()` and the package version into
   `artifacts/run-metadata.json` — any artifact must be regenerable from raw data via
   `Eventstream.from_recipe(raw_df, recipe)`.

### 5. Validate before presenting

Work through `references/gotchas-and-validation.md` section B. Non-negotiables:
every percentage names its denominator; population filters are disclosed with counts;
survivorship and exposure confounds addressed; no outcome leakage into features;
small cells flagged with n; caption numbers come from headless `*_data` twins;
visuals agree with tables.

### 6. Interpret and deliver

Structure the final answer as:

1. **Observed** — numbers with denominators and n.
2. **Interpretation** — what it likely means.
3. **Alternative explanations** — selection, structure, censoring.
4. **Product hypotheses** — each with the metric an experiment would move.
5. **Suggested next analyses / A-B tests.**
6. **Limitations.**

Deliverables: analysis script or executed notebook; `artifacts/data-profile.json`;
`artifacts/metrics.csv` (key tables); interactive HTML exports via
`widget.export_html(..., title=, analysis=)` — write `analysis=` captions AFTER
conclusions are final; `artifacts/summary.md` (mapping, filters, assumptions, versions,
findings, limitations, next steps); `artifacts/run-metadata.json` (versions, parameters,
seeds, `recipe()` lineage).
