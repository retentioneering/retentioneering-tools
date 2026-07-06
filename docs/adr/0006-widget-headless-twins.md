# ADR-0006: Widget / headless `*_data` twin pattern

Status: Accepted (5.0 rewrite; alias added 2026-07)

## Context

The same computations serve two audiences: analysts exploring interactively
in Jupyter, and pipelines/agents that need raw numbers (custom viz, exports,
ML features, MCP summaries).

## Decision

- Every widget `stream.X()` has a headless twin `stream.X_data()` that runs
  the same computation and returns plain data (DataFrame/dict) instead of
  rendering.
- A widget's parameters split into **data parameters** — by definition,
  exactly the signature of its `*_data` twin — and **display parameters**
  (`height`, `sidebar_open`, ...). Docs render the two groups separately and
  state the rule.
- Step Matrix and Step Sankey render the same per-step matrices, so they
  share one computation; `step_matrix_data()` exists as a documented alias
  of `step_sankey_data()` so the naming pattern never breaks (a user or LLM
  who used `step_matrix()` will guess `step_matrix_data()` and must not get
  an AttributeError).
- Headless computation classes live in `tools/` (`TransitionMatrix`,
  `StepMatrix`, `Funnel`, `SegmentOverview`, `ClusterAnalysis`); widgets in
  `widgets/` call through the Eventstream `*_data` methods, never into
  `tools/` directly.

## Consequences

- Any new widget must ship with its `_data` twin and keep signatures in
  lockstep; the docs pipeline (ADR-0013) renders both from docstrings and
  makes drift visible.
- The MCP server (ADR-0009) builds on the same twins: it constructs widgets
  for report tabs but summarizes from the same underlying data.
