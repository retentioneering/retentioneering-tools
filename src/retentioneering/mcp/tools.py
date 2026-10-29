"""MCP agent tools as plain, transport-independent functions.

Each function here is `(session, **params) -> dict` — no MCPServer, no JSON
serialization, no closure over server-lifetime state. `server.py` wraps each
one as a thin `@mcp.tool()` adapter (`json.dumps(tools.foo(session, ...))`)
for the notebook flow; any other caller (e.g. the platform's chat assistant,
via `client.beta.messages.tool_runner`) can call these directly against its
own `ReportSession` (or a subclass overriding `update_base_stream`/
`reset_base_stream` to fork/reset a persisted DAG instead of an in-memory
stream — see the platform's `PlatformAgentSession`). These functions only
touch `session`'s public protocol (`active_stream`, `context_events`,
`update_base_stream()`, `reset_base_stream()`, `add_tab()`, `pending_tabs`,
`package()`), so any object shaped like `ReportSession` works.

Docstrings here are the ones MCPServer quotes as tool descriptions for the
notebook/MCP-client flow (see ADR-0009) — the platform's own `@beta_tool`
adapters in `apps/api/app/assistant.py` write their own Google-style
docstrings rather than reusing these numpy-style ones verbatim.
"""

from __future__ import annotations

import inspect
import json
import math
import warnings
from typing import Any

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp._agent_logic import (
    _apply_preprocessors,
    _find_unlinked_numbers,
    _segment_overview_summary,
    _step_matrix_summary,
    _transition_graph_summary,
)
from retentioneering.mcp._prompts import (
    _ANALYSIS_METHODS,
    _PLAYBOOK,
    _STATIC_TOOL_DOCS,
    _playbook_index,
    _tool_docs_index,
)
from retentioneering.mcp._report_session import ReportSession


def _require_stream(session: Any) -> dict | None:
    """Guard for tools that operate on the active stream. Returns an error
    dict if serve() was started without a stream and load_data() hasn't been
    called yet; None if a stream is present and the caller can proceed.
    """
    if session.active_stream is None:
        return {
            "error": (
                "No eventstream loaded yet. Call load_data(path=..., schema=...) "
                "first to load a CSV file into this session."
            )
        }
    return None


def load_data(
    session: Any,
    path: str,
    schema: dict | None = None,
    context: dict | None = None,
) -> dict:
    """
    Load an eventstream from a local CSV file and set it as the base stream
    for this session, replacing whatever stream is currently active (from
    serve() or an earlier load_data call).

    Call this FIRST, before any other tool, if serve() was started without a
    stream (data-agnostic mode) — every other tool returns an error until
    this succeeds. Also usable later to switch the session to an entirely
    different dataset.

    Parameters
    ----------
    path:
        Path to a CSV file, readable by the process running the MCP server
        (same filesystem as wherever serve() was invoked from).
    schema:
        Optional column mapping:
          {"path_cols": [...], "event_col": "...", "timestamp_col": "...",
           "segment_cols": [...]}
        Defaults to path_cols=["user_id"], event_col="event",
        timestamp_col="timestamp" for any key left unspecified.
    context:
        Optional semantic layer — same shape as serve()'s context argument
        (description, events, kpis). Replaces any context set previously.

    Returns
    -------
    Stream stats: n_paths, n_events_total, events list.
    {"error": ...} if the file can't be read or doesn't match the schema.
    """
    try:
        stream = Eventstream(path, schema=schema)
    except Exception as exc:
        return {"error": str(exc)}
    session.load_data(stream, context)
    return {"status": "data loaded", **ReportSession.stream_stats(stream)}


def update_base_stream(session: Any, preprocessors: list) -> dict:
    """
    Apply preprocessing to the original eventstream and set it as the active
    base stream for the session. All subsequent add_* calls will use this stream.

    Call this only when the user explicitly wants to change the analysis baseline
    (e.g. "filter short paths and analyse"). Always confirm with the user before
    calling — it replaces the current context.

    Always applies to the ORIGINAL stream from serve(), not the current active one,
    so repeated calls never stack filters accidentally.

    Parameters
    ----------
    preprocessors:
        Ordered list of steps. Each step is {"type": "<name>", ...args}.
        Available types: collapse_events, filter_paths, filter_events, truncate_paths,
        rename_events, edit_events, drop_events, add_events, to_daily_states,
        add_segment, drop_segment, add_clusters, urls_to_events, sample_paths,
        split_sessions.
        Call describe_tool("<type>") for full parameter reference before using any step.

    Returns
    -------
    Stream stats: n_paths, n_events_total, events list.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        new_stream = session.update_base_stream(preprocessors)
    except Exception as exc:
        return {"error": str(exc)}
    return {"status": "base stream updated", **ReportSession.stream_stats(new_stream)}


def reset_base_stream(session: Any) -> dict:
    """
    Reset the active stream to the original eventstream passed to serve().
    Use this when the user wants to start a new analysis branch from scratch
    or undo all preprocessing.

    Returns updated stream stats.
    """
    if (err := _require_stream(session)) is not None:
        return err
    new_stream = session.reset_base_stream()
    return {
        "status": "base stream reset to original",
        **ReportSession.stream_stats(new_stream),
    }


def playbook(scenario: str = "") -> dict:
    """
    Return the canonical recipe for a named analysis scenario.
    Call this when you recognise a pattern in the user's question but need
    the step-by-step procedure. Do NOT improvise — look it up here first.

    Pass "" (empty) to list all available scenarios.
    """
    return _PLAYBOOK.get(scenario.strip(), _playbook_index())


def describe_tool(tool: str = "") -> dict:
    """
    Return full parameter documentation for a preprocessor type or internal tool.
    Call this before using any preprocessor you are not 100% sure about —
    do NOT guess parameters or read source files.

    Pass "" (empty) to list all documented tools.

    Preprocessors (use as {"type": "<name>", ...} in update_base_stream / local_preprocessors):
      collapse_events  filter_paths   filter_events   truncate_paths
      rename_events    edit_events    drop_events     add_events
      to_daily_states  add_segment    drop_segment    add_clusters
      urls_to_events   sample_paths   split_sessions

    Analysis tools (called directly, not as a preprocessor step):
      get_conversion_rate  get_sample_ratio  get_ab_test  get_ab_bootstrap

    Reference topics:
      report_links   (anchor link syntax for analysis text)
      metrics        (the metric registry: names, metric_args, how A/B reads them)
    """
    t = tool.strip()
    if not t:
        return _tool_docs_index()
    if t in _STATIC_TOOL_DOCS:
        return {"topic": t, "docs": _STATIC_TOOL_DOCS[t]}
    method = getattr(Eventstream, t, None)
    doc = inspect.getdoc(method) if method and callable(method) else None
    if doc:
        if t in _ANALYSIS_METHODS:
            out: dict[str, Any] = {"method": t}
            python_only = _python_only_parameters(t)
            if python_only:
                out["through_mcp"] = (
                    "The docs below are the Python method's. Through MCP these "
                    f"parameters are NOT available: {', '.join(python_only)}. "
                    "Passing one gives an 'unexpected keyword argument' error."
                    + (
                        " assignment= comes from serve(stream, assignment=...) "
                        "when the session has a table (see describe())."
                        if "assignment" in python_only
                        else ""
                    )
                )
            out["docs"] = doc
            return out
        return {"preprocessor": t, "docs": doc}
    return {"error": f"Unknown tool {t!r}.", **_tool_docs_index()}


def _python_only_parameters(name: str) -> list[str]:
    """Parameters of ``Eventstream.<name>`` that the MCP tool of the same name
    does not take."""
    tool = globals().get(name)
    if tool is None:
        return []
    python = list(inspect.signature(getattr(Eventstream, name)).parameters)[1:]
    mcp = set(inspect.signature(tool).parameters)
    return [p for p in python if p not in mcp]


def describe(session: Any, context: dict) -> dict:
    """Return schema, event list, unique path counts and available segments.
    Reflects the current active stream (after any update_base_stream calls)."""
    if (err := _require_stream(session)) is not None:
        return err
    cur = session.active_stream
    s = cur.schema
    ec, pc = s.event_col, s.path_col
    df = cur.df
    ts_col = s.timestamp_col
    result = {
        "n_paths": int(df[pc].nunique()),
        "n_events_total": len(df),
        "event_col": ec,
        "path_col": pc,
        "path_cols": s.path_cols,
        "segment_cols": s.segment_cols,
        "timestamp_col": ts_col,
        "date_range": {
            "min": str(df[ts_col].min())[:10],
            "max": str(df[ts_col].max())[:10],
        },
        "events": sorted(df[ec].astype(str).unique().tolist()),
    }
    result.update(_experiment_facts(cur))
    assignment = getattr(session, "assignment", None)
    if assignment is not None:
        result["assignment"] = _assignment_facts(assignment)
    if context.get("events"):
        result["event_descriptions"] = context["events"]
    if context.get("segments"):
        result["segment_descriptions"] = context["segments"]
    return result


#: describe() lists at most this many levels per segment column.
_MAX_LEVELS = 20


def _experiment_facts(stream: Eventstream) -> dict:
    """What an A/B call needs from describe(): segment levels (arm names), value
    columns (revenue) and which segments are constant per path (an arm or a
    stratum must be; constant per user means fixed for the user)."""
    from retentioneering.tools._diff import MULTI, per_path_label

    schema = stream.schema
    df = stream.df
    levels, truncated = {}, []
    for col, values in stream.get_segment_levels().items():
        values = [str(v) for v in values]
        if len(values) > _MAX_LEVELS:
            truncated.append(col)
        levels[col] = values[:_MAX_LEVELS]
    constant: dict[str, list[str]] = {}
    for col in schema.segment_cols:
        constant[col] = [
            path_col
            for path_col in schema.path_cols
            if not (per_path_label(df, col, path_col) == MULTI).any()
        ]
    values = [
        col
        for col in (schema.custom_cols or [])
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col])
    ]
    out: dict[str, Any] = {
        "segment_levels": levels,
        "segments_constant_per_path": constant,
        "value_columns": values,
    }
    if truncated:
        out["segment_levels_truncated"] = truncated
    return out


def _assignment_facts(assignment: Any) -> dict:
    """The session's assignment table in a few numbers (never the table)."""
    from retentioneering.tools._diff import normalize_assignment

    try:
        table = normalize_assignment(assignment)
    except Exception as exc:  # reported, not raised: describe() must answer
        return {"error": str(exc)}
    arms = table["arm"].astype(str).value_counts().sort_index()
    out: dict[str, Any] = {
        "paths": int(len(table)),
        "arms": {str(k): int(v) for k, v in arms.items()},
        "has_assigned_at": "assigned_at" in table.columns,
        "usage": (
            "get_sample_ratio and get_ab_test count every assigned path (silent ones "
            "included, intention to treat): diff=['arm', <arm1>, <arm2>]"
        ),
    }
    if isinstance(assignment, pd.DataFrame):
        out["columns"] = [str(c) for c in assignment.columns]
    return out


def add_transition_graph(
    session: Any,
    label: str,
    edge_weight: str = "proba_out",
    diff: list | None = None,
    path_col: str | None = None,
    views: list | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a transition graph and register it as a tab in the pending report.
    Returns the transition matrix so you can analyse it before calling export_report().

    Call this (possibly multiple times with different parameters) before
    calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. Use a short descriptive name,
        e.g. "Overall Flow" or "Mobile vs Desktop". No colons in the label.
    edge_weight:
        How to weight edges. One of: proba_out, proba_in, count, unique_paths,
        share_of_total, avg_per_path, time_median, time_q95.
    diff:
        Optional diff: [segment_col, value1, value2].
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, events list, values matrix (and group1/group2 in diff mode).
    Use tab_id to reference this tab in the analysis text as [label:event].

    views:
        Optional named visual presets (focus/filters/viewport only — never
        data parameters), rendered as pills and addressable from analysis
        text as [label:view=Name].

    local_preprocessors:
        Optional one-off preprocessing applied on top of the base stream for
        this visualisation only. Same format as update_base_stream preprocessors.
        Use sparingly — prefer update_base_stream when the same preprocessing
        applies to multiple visualisations.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    widget = src.transition_graph(
        edge_weight=edge_weight,
        diff=diff,
        path_col=path_col or None,
        **({"views": views} if views else {}),
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "transition_graph",
        "widget_id": widget.widget_id,
        "result": json.loads(widget.result or "{}"),
        "edge_weight": widget.edge_weight,
        "diff": json.loads(widget.diff) if widget.diff else None,
        "event_counts": json.loads(widget.event_counts or "{}"),
        "event_counts_g1": json.loads(widget.event_counts_g1 or "{}"),
        "event_counts_g2": json.loads(widget.event_counts_g2 or "{}"),
        "node_positions": widget.semantic_layout_positions(),
        "event_visibility": {},
        "views": json.loads(widget.views or "[]"),
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "path_cols": json.loads(widget.path_cols or "[]"),
        "path_col": widget.path_col or "",
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _transition_graph_summary(result_raw, session.context_events, edge_weight)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def add_step_matrix(
    session: Any,
    label: str,
    max_steps: int = 10,
    diff: list | None = None,
    path_pattern: str | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a step matrix and register it as a tab in the pending report.
    Returns the matrix data so you can analyse it before calling export_report().

    Call this (possibly multiple times) before calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. No colons in the label.
    max_steps:
        Maximum steps before/after anchor.
    diff:
        Optional diff: [segment_col, value1, value2].
    path_pattern:
        Filter paths by an event sequence: "add_to_cart->.*->purchase", or
        "add_to_cart->[^support_chat]*->purchase" for the same without support
        in between, "add_to_cart->.{0,4}->purchase" for at most four events in
        between, "(cart->.*){2}->purchase" for two carts before it (each copy is
        its own block). Full syntax: the Path Patterns docs page.
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, label, and a summary: top events per step (a `blocks` list, one per
    segment, when path_pattern is set), plus full rows for context events.

    local_preprocessors: same as in add_transition_graph.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    widget = src.step_matrix(
        max_steps=max_steps,
        diff=diff,
        path_col=path_col or None,
        path_pattern=path_pattern or None,
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "step_matrix",
        "result": json.loads(widget.result or "{}"),
        "max_steps": widget.max_steps,
        "diff": json.loads(widget.diff) if widget.diff else None,
        "path_col": widget.path_col or "",
        "path_pattern": widget.path_pattern or "",
        "path_cols": json.loads(widget.path_cols or "[]"),
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "event_list": json.loads(widget.event_list or "[]"),
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _step_matrix_summary(result_raw, session.context_events)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def add_segment_overview(
    session: Any,
    label: str,
    segment_col: str,
    metrics: list | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a segment overview and register it as a tab in the pending report.
    Returns the overview table so you can analyse it before calling export_report().

    Call this (possibly multiple times with different segment columns) before
    calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. No colons in the label.
    segment_col:
        Column to segment by (must be listed in segment_cols from describe()).
    metrics:
        List of additional metric dicts. segment_size and segment_share are
        always computed automatically and do not need to be specified.

        Each dict: {"metric": <name>, "metric_args": {...}, "agg": <agg>}
        "agg" choices: "mean" (default), "median", "complement_distance",
                       "q5", "q25", "q75", "q95"

        Available metrics and their metric_args:
          {"metric": "length"}
              — number of events per path
          {"metric": "duration"}
              — duration in seconds (first to last event)
          {"metric": "event_count", "metric_args": {"event": "purchase"}}
              — how many times the event occurred (single event only; use
                "event_count_bulk" with "events": [...] for one column per event)
          {"metric": "has_event", "metric_args": {"event": "purchase"}}
              — 0/1 whether the path contains the event (conversion rate)
          {"metric": "time_between",
           "metric_args": {"start_event": "add_to_cart", "end_event": "purchase"}}
              — seconds between first occurrences of two events
          {"metric": "matches_pattern",
           "metric_args": {"pattern": "add_to_cart->.*->purchase"}}
              — 0/1 whether path matches the pattern
          {"metric": "in_segment",
           "metric_args": {"segment_name": "channel", "segment_level": "mobile"}}
              — 0/1 whether the path belongs to that segment level;
                "mode": "any" (default) / "all" / "event_share" (+ "threshold")
          {"metric": "in_segment_bulk", "metric_args": {"segment_name": "channel"}}
              — one in_segment column per level of that segment column;
                omit "segment_name" for every level of every segment column

        Examples:
          Conversion rate to purchase by platform:
            metrics=[
              {"metric": "has_event",  "metric_args": {"event": "purchase"}, "agg": "mean"},
              {"metric": "length"},
              {"metric": "duration", "agg": "median"},
            ]
          Time-to-purchase by acquisition channel:
            metrics=[
              {"metric": "time_between",
               "metric_args": {"start_event": "home", "end_event": "purchase"},
               "agg": "median"},
            ]
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, metrics list, segments list, values matrix.

    local_preprocessors: same as in add_transition_graph.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    widget = src.segment_overview(
        segment_col=segment_col,
        metrics=metrics or [],
        path_col=path_col or None,
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "segment_overview",
        "result": json.loads(widget.result or "{}"),
        "segment_col": widget.segment_col or "",
        "path_col": widget.path_col or "",
        "metrics": json.loads(widget.metrics or "[]"),
        "segment_cols": json.loads(widget.segment_cols or "[]"),
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "path_cols": json.loads(widget.path_cols or "[]"),
        "event_list": json.loads(widget.event_list or "[]"),
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _segment_overview_summary(result_raw, session.context_events)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def add_ab_test(
    session: Any,
    label: str,
    diff: list,
    metrics: list,
    strata_col: str | None = None,
    period: str | None = None,
    period_population: str = "itt",
    correction: str = "holm",
    ci_level: float = 0.95,
    path_col: str | None = None,
    strata_pre_exposure: bool | None = None,
    local_preprocessors: list | None = None,
    horizon: str | None = None,
    period_basis: str = "calendar",
    test: str = "auto",
    correction_scope: str = "metric",
) -> dict:
    """
    Compute an A/B test scorecard and register it as a tab in the pending report.

    The tab is the ab_test widget: a table and a forest plot of each metric's
    delta with its interval, coloured by verdict, plus Strata and Periods tabs
    with strata_col / period. Its numbers are get_ab_test's (same arguments, same
    population: the session's assignment table when there is one, otherwise the
    stream's paths split by the segment in `diff`). Run get_sample_ratio first.
    Use get_ab_test instead when you only need numbers; a numbers-only analysis
    still needs one tab to export a report, and this is the A/B one.

    Parameters
    ----------
    label:
        Tab label shown in the report. No colons in the label.
    diff, metrics, strata_col, period, period_population, correction, ci_level,
    path_col, strata_pre_exposure, local_preprocessors, horizon, period_basis,
    test, correction_scope:
        As in get_ab_test.

    Returns
    -------
    tab_id, label, and metrics: per metric (overall rows) verdict, delta,
    delta_ci_low, delta_ci_high, p_value_adjusted, mde, support_warning and
    reading (when the library provides it); warnings. Link a metric's row in
    the analysis as [label:metric_name]. "inconclusive" is NOT "no effect".
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    try:
        widget = src.ab_test(
            tuple(diff) if diff else None,
            metrics,
            assignment=getattr(session, "assignment", None),
            strata_col=strata_col or None,
            period=period or None,
            period_basis=period_basis if period else None,
            period_population=period_population if period else None,
            horizon=horizon or None,
            test=test,
            correction=correction,
            correction_scope=correction_scope,
            ci_level=ci_level,
            path_col=path_col or None,
            strata_pre_exposure=strata_pre_exposure,
            sidebar_open=False,
        )
    except Exception as exc:
        return {"error": str(exc), "label": label}
    if widget.error:
        return {"error": widget.error, "label": label}
    data = widget._export_data(sidebar_open=False)
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result = data["result"]
    entries = []
    for row in result.get("overall") or []:
        entry = {"metric": row["metric"], "verdict": row["verdict"]}
        for column in ("delta", "delta_ci_low", "delta_ci_high", "mde"):
            entry[column] = _json_number(row.get(column), 6)
        entry["p_value_adjusted"] = _json_number(row.get("p_value_adjusted"), 8)
        entry["support_warning"] = row.get("support_warning", "")
        if "reading" in row:
            entry["reading"] = row["reading"]
        entries.append(entry)
    return {
        "tab_id": tab_id,
        "label": label,
        "metrics": entries,
        "warnings": result.get("warnings", []),
    }


def get_conversion_rate(
    session: Any,
    start_anchor: str | dict | list,
    end_anchor: str | dict | list,
    within: int | str | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
    ci_level: float | None = None,
    diff: list | None = None,
) -> dict:
    """
    Measure how often one event is followed by another, against its baseline.

    Answers "given that Y happened, how often does X follow — and is that
    different from usual?" for one pair of events, or a fan of them.

    Returns numbers only: unlike add_transition_graph / add_step_matrix /
    add_segment_overview this registers NO tab in the report, so every figure
    you quote from it is a number you computed — wrap those in backticks in the
    analysis text (`30.0%`, `2.3x`), which check_analysis exempts from the
    anchor-link requirement.

    Prefer this over reading a pair off a transition graph: a graph edge is a
    share of *transitions* between adjacent events, this is a share of *paths*
    where one event was followed by another at any distance. They answer
    different questions and rarely agree.

    Parameters
    ----------
    start_anchor:
        The condition. An event name, or an anchor spec
        {"pattern": ..., "at": ..., "occurrence": ..., "offset": ...} — the
        same specs truncate_paths takes; call describe_tool("truncate_paths")
        for the spec's keys. A LIST means several separate questions, one row
        each — NOT one anchor assembled from several parts.
    end_anchor:
        The target(s) looked for after it, same forms. "path_start" /
        "path_end" are ordinary names, so end_anchor="path_end" with within=1
        is an exit rate ("the path ended right after Y").
    within:
        Window measured from the start anchor, far edge included. An int counts
        events (10 = "within 10 events of Y"), a string counts time ("30m",
        "2h"). Omit to look to the end of the path.
    path_col:
        Override the path ID column. Pass "session_id" (when the schema has it,
        see describe()) if the question is about a visit rather than a person —
        per-user conversion is often washed out by everything the user ever did.
    local_preprocessors:
        Optional one-off preprocessing applied on top of the base stream for
        this measurement only. Same format as update_base_stream preprocessors.
    ci_level:
        e.g. 0.95: adds conversion_rate_ci_low / conversion_rate_ci_high (Wilson).
    diff:
        Compare two groups of paths: ["segment_col", "value1", "value2"]
        ("<REST>" allowed as value2). Each row then carries group1_* and group2_*
        versions of the columns below (with their CIs), delta (group1 - group2),
        delta_ci_low / delta_ci_high and p_value. Groups are whole paths: a path
        whose segment changes inside it is an error, not a split. The interval
        assumes path_col is the unit that was assigned (user-level test -> user
        paths). The denominator is the paths with the start anchor: unless that is
        "path_start", each row is conditional=true, the groups are selected after
        exposure and the p_value describes, it does not test; for the treatment's
        effect use get_ab_test with a matches_pattern metric (an ITT share).

    Returns
    -------
    path_col, within, ci_level, diff (echoed back), and rows — one per (start, end) pair:
      paths_with_start  the denominator: paths where the start anchor occurred
      converted         of those, paths where the target followed it
      conversion_rate   converted / paths_with_start (null if denominator is 0)
      base_rate         share of ALL paths where the target occurs anywhere
      lift              conversion_rate / base_rate (null if base_rate is 0)

    ALWAYS report the denominator and the lift next to the rate. A rate alone
    hides how many paths it is about, and whether it beats simply being a
    common event: lift below 1 means the start event makes the target LESS
    likely, which is usually the finding.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    try:
        frame = src.get_conversion_rate(
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            within=within,
            path_col=path_col or None,
            ci_level=ci_level,
            diff=tuple(diff) if diff else None,
        )
    except Exception as exc:
        return {"error": str(exc)}

    def _num(value: Any) -> float | None:
        # NaN is a real answer here (no paths reached the start, or the target
        # never occurs at all) and has to survive as JSON null, not as the
        # bare NaN token json.dumps would otherwise emit.
        value = float(value)
        return None if math.isnan(value) else round(value, 4)

    def _cell(column: str, value: Any) -> Any:
        if column in ("start_anchor", "end_anchor"):
            return str(value)
        if column.endswith(("paths_with_start", "converted")):
            return int(value)
        if column == "p_value":
            return _json_number(value, 8)
        if column == "conditional":
            return bool(value)
        return _num(value)

    out: dict[str, Any] = {
        "path_col": path_col or src.schema.path_col,
        "within": within,
        "rows": [
            {column: _cell(column, value) for column, value in row.items()}
            for row in frame.to_dict("records")
        ],
    }
    if ci_level is not None:
        out["ci_level"] = ci_level
    if diff:
        out["diff"] = list(diff)
    return out


def _json_number(value: Any, digits: int = 6) -> float | None:
    """A float for JSON: NaN/inf become null (an absent estimate is not 0)."""
    if value is None:
        return None
    value = float(value)
    return round(value, digits) if math.isfinite(value) else None


def get_sample_ratio(
    session: Any,
    segment_col: str | None = None,
    expected_shares: dict | None = None,
    alpha: float = 0.001,
    path_col: str | None = None,
    strata_col: str | None = None,
    period: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Check an A/B test's arm sizes against the expected split (sample ratio mismatch).

    Run this FIRST on any experiment, before reading a single metric: if the
    arms do not have the planned sizes, paths were lost from one arm and every
    comparison is suspect. Returns numbers only (no report tab); quote them in
    backticks.

    With an assignment table in the session (serve(stream, assignment=...);
    describe() shows it) the counts are the table's rows, silent assigned users
    included, and segment_col is optional: omit it to check the table alone, or
    name the stream's arm segment to also check that every path agrees with the
    table.

    Parameters
    ----------
    segment_col:
        Segment column holding the arm (see describe() for segments). Required
        without an assignment table.
    expected_shares:
        {arm: share}, summing to 1. Omit for an equal split.
    alpha:
        Mismatch threshold, default 0.001 (SRM alarms are strict on purpose).
    path_col:
        The unit that was randomised (default: the primary path column).
    strata_col:
        Repeat the check per stratum: a segment constant per path that was fixed at
        assignment (platform at signup). Not for variables the treatment can change.
    period:
        Repeat the check per calendar unit of assigned_at (with an assignment
        table) or of the path's first event: "D" (day), "W" (week) or "M"
        (month), as get_ab_test spells it. Not with strata_col.
    local_preprocessors:
        One-off preprocessing for this check only. Never filter on behaviour
        (e.g. "reached cart") before an SRM check: on such a subset a share
        difference is a treatment effect, not a bug.

    Returns
    -------
    segment_col, path_col, mismatch (bool), p_value, chi2, dof, alpha,
    multi_valued_paths (paths with >1 arm: an assignment bug), silent_paths
    (assigned paths with no events; 0 without a table), unassigned_paths,
    basis, reading (one sentence: the expected and observed split, the p-value and
    the conclusion, e.g. "expected 50/50, observed 5040/4960 (control/treatment),
    p 0.42: no mismatch"; quote it), and levels — one row per arm with paths, share,
    expected_share. With strata_col: strata, one row per stratum with stratum, paths,
    chi2, p_value, p_value_adjusted (Holm across the strata) and mismatch; with
    period: periods, the same per period; reading names the strata or periods that
    mismatch.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    try:
        result = src.get_sample_ratio(
            segment_col or None,
            expected_shares=expected_shares,
            alpha=alpha,
            path_col=path_col or None,
            assignment=getattr(session, "assignment", None),
            strata_col=strata_col or None,
            period=period or None,
        )
    except Exception as exc:
        return {"error": str(exc)}

    levels = result["levels"]
    for key in ("stratum", "period"):
        if key in levels.columns:
            levels = levels[levels[key] == "<ALL>"]
    out: dict[str, Any] = {
        "segment_col": segment_col,
        "path_col": path_col or src.schema.path_col,
        "mismatch": bool(result["mismatch"]),
        "p_value": _json_number(result["p_value"], 8),
        "chi2": _json_number(result["chi2"], 4),
        "dof": int(result["dof"]),
        "alpha": float(result["alpha"]),
        "multi_valued_paths": int(result["multi_valued_paths"]),
        "silent_paths": int(result["silent_paths"]),
        "unassigned_paths": int(result["unassigned_paths"]),
        "basis": result["basis"],
        "reading": result["reading"],
        "levels": [
            {
                "level": str(row["level"]),
                "paths": int(row["paths"]),
                "share": _json_number(row["share"], 4),
                "expected_share": _json_number(row["expected_share"], 4),
            }
            for row in levels.to_dict("records")
        ],
    }
    for arg, key, name in (
        ("strata_col", "stratum", "strata"),
        ("period", "period", "periods"),
    ):
        if name not in result:
            continue
        out[arg] = result[arg]
        out[name] = [
            {
                key: str(row[key]),
                "paths": int(row["paths"]),
                "chi2": _json_number(row["chi2"], 4),
                "p_value": _json_number(row["p_value"], 8),
                "p_value_adjusted": _json_number(row["p_value_adjusted"], 8),
                "mismatch": bool(row["mismatch"]),
            }
            for row in result[name].to_dict("records")
        ]
    return out


def _scope_text(
    correction: str, correction_scope: str, rows: str = "this metric's rows"
) -> str:
    """What one row's p_value_adjusted and interval are corrected against."""
    tail = (
        "intervals are per comparison (unadjusted); the verdict reads "
        "p_value_adjusted"
    )
    if correction == "none":
        return (
            "no correction: p_value_adjusted = p_value; metrics are not corrected "
            f"against each other; {tail}"
        )
    if correction_scope == "all":
        return f"{correction} across every row of every metric; {tail}"
    return (
        f"{correction} within {rows} only; metrics are not corrected against each "
        f"other (name one decision metric); {tail}"
    )


def get_ab_test(
    session: Any,
    diff: list,
    metrics: list,
    strata_col: str | None = None,
    period: str | None = None,
    period_population: str = "itt",
    correction: str = "holm",
    ci_level: float = 0.95,
    path_col: str | None = None,
    strata_pre_exposure: bool | None = None,
    local_preprocessors: list | None = None,
    horizon: str | None = None,
    period_basis: str = "calendar",
    test: str = "auto",
    correction_scope: str = "metric",
) -> dict:
    """
    Compare two arms of an experiment on path metrics: delta, interval, verdict, MDE.

    Run get_sample_ratio first: if the arms do not have the planned sizes, no
    metric of the test can be trusted. Returns numbers only (no report tab): quote
    them in backticks. Population (the `population` field): with an assignment
    table in the session (serve(stream, assignment=...), shown by describe()),
    every assigned user, silent ones included (intention to treat, "assignment");
    without one, the paths of the stream (users with at least one event) split by
    the segment in `diff` ("stream"): silent assigned users are not counted, and
    horizon / since-assignment periods need the table's assigned_at.

    Parameters
    ----------
    diff:
        ["segment_col", "group1", "group2"], e.g. ["exp_group", "treatment",
        "control"]; delta = group1 - group2. Exactly three elements. With an
        assignment table, diff[0] is "arm" (the table) or the stream's arm segment.
        The segment must be constant per path, and path_col must be the randomised
        unit (per-user experiment -> user paths; session paths raise an error).
    metrics:
        Registry metric configs, one number per path, each with an optional "name"
        and "denominator" (a second config: the metric becomes a ratio of sums, e.g.
        revenue / orders = average order value). describe_tool("metrics") lists
        the registry. Examples:
        {"name": "conversion", "metric": "has_event", "metric_args": {"event": "purchase"}},
        {"name": "revenue", "metric": "column_sum", "metric_args": {"column": "amount"}}.
        Metric arguments go under "metric_args", never at the top level.
    strata_col:
        A segment constant per path and fixed BEFORE exposure (platform at signup, a
        cluster found on a stream truncated before the first exposure). Adds the top
        three strata by |delta| per metric (strata_selection says so: picking the
        largest deltas is selection, read them as exploratory) with p-values adjusted
        with `correction`, a stratum-vs-rest p-value and a heterogeneity p-value. A
        stratum defined by post-exposure behaviour compares selected groups, not
        randomised ones: strata_pre_exposure=false and exploratory=true flag it.
    strata_pre_exposure:
        true declares the strata fixed before exposure (platform at signup); omit to
        read the recipe (a declaration add_segment(..., pre_exposure=true), or a
        truncate_paths that cuts at an event before the segment was computed).
    period:
        Calendar unit ("D", "W", "M", "Q") or fixed length ("7D"): adds one row per
        period. ITT by default (every path that entered before the period's end
        counts in it, 0 when silent).
    period_basis:
        "calendar" (default) or "since_assignment" (bins of time since assigned_at,
        "0-6", "7-13", ...: the novelty view; needs the session's assignment table
        with assigned_at; period is then a fixed length such as "7D").
    period_population:
        "itt" (default, inferential) or "active" (only paths active in the period —
        survivors; rows are descriptive, no p-value).
    horizon:
        A fixed window per user, e.g. "14D" or "28D": only events in the first 14
        days after assigned_at count, and users not observed that long are left out
        (immature). Needs the session's assignment table with assigned_at. Different
        horizons answer different questions: choose it before reading.
    test:
        "auto" (default: z-test for 0/1 metrics, Welch for totals and values,
        bootstrap for ratios), "z_test", "welch_t_test" or "bootstrap".
    correction:
        "holm" (default), "bonferroni", "bh" or "none".
    correction_scope:
        "metric" (default): the correction runs within one metric's rows, metrics
        are not corrected against each other — name one decision metric. "all":
        every row of every metric in one family (a declared set of guardrails).
    ci_level:
        Confidence level of the intervals (default 0.95).
    path_col:
        The randomised unit (default: the primary path column).
    local_preprocessors:
        One-off preprocessing. Never filter on post-exposure behaviour (e.g. "reached
        checkout") before a test: that compares selected groups, not the arms.

    Returns
    -------
    diff, path_col, ci_level, correction, correction_scope, population
    ("assignment" or "stream"), unassigned_paths (stream paths in neither
    group of diff, e.g. a third arm), and metrics — one entry per
    metric: population, group1_paths, group2_paths, group1_support,
    group2_support (paths with a value), conditional (true when the metric has a
    value only for some paths — time_between, column_mean, order value: it
    compares those paths, not the arms), group1_value, group2_value, delta,
    delta_ci_low, delta_ci_high, rel_delta, p_value (raw), p_value_adjusted, mde
    (the effect the test detects with 80% power at mde_alpha), verdict
    ("increase", "decrease", "inconclusive", "descriptive", "insufficient_data"),
    test, correction_family (what was adjusted together), mde_alpha (the level the
    mde is computed at, alpha / m under a correction; the verdict compares
    p_value_adjusted with 1 - ci_level), reading (one sentence built from the
    row's numbers: the window it covers, what the interval includes or excludes
    after the correction, the p-value, for an inconclusive row the effect the
    design detects and the paths needed for a smaller one, and the row's flags;
    quote it), unit ("pp": a 0/1 metric whose delta fields are shares and whose
    reading is in percentage points; "value": the metric's own unit) and scope
    (what the row is corrected against: e.g. "holm within this metric's rows
    only; metrics are not corrected against each other ...; intervals are per
    comparison (unadjusted)"); with strata_col: strata (top 3 by |delta|:
    stratum, group1_share, group2_share, delta, delta_ci_low, delta_ci_high,
    p_value, p_value_adjusted, mde_alpha, stratum_vs_rest_p_value, verdict,
    reading, unit, scope), strata_selection ("top 3 by |delta| of N"),
    strata_count, heterogeneity_p_value, strata_correction_family,
    strata_pre_exposure, strata_pre_exposure_source ("declared", "recipe",
    "not_pre_exposure") and exploratory (true unless the strata count as
    pre-exposure); support_warning per metric ("no_data": no path of either arm
    has the behaviour, or no value in one arm for a value or ratio metric;
    "one_arm_only": the behaviour exists in one arm only; "invalid_replicates":
    more than 1 % of bootstrap replicates without a value in an arm, so no
    interval or p-value; "low_support": few paths behind a bootstrap interval,
    coverage below nominal);
    warnings (the library's warnings for this call); with period: periods (period,
    period_complete, delta, delta_ci_low, delta_ci_high, p_value,
    p_value_adjusted, mde_alpha, verdict, reading, unit, scope) and
    periods_correction_family; with horizon: horizon and immature_paths.
    "descriptive" marks period_population="active" rows (survivors, no p-value).

    "inconclusive" is NOT "no effect": quote the row's `reading` (it says which
    effects the interval still allows and what the design could detect) and propose
    the next step; the mde is not a bound on the effect.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    diff_t = tuple(diff) if diff else None
    assignment = getattr(session, "assignment", None)
    kwargs = {
        "assignment": assignment,
        "horizon": horizon or None,
        "test": test,
        "correction": correction,
        "correction_scope": correction_scope,
        "ci_level": ci_level,
        "path_col": path_col or None,
    }
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            overall = src.get_ab_test(diff_t, metrics, **kwargs)
            strata = (
                src.get_ab_test(
                    diff_t,
                    metrics,
                    strata_col=strata_col,
                    strata_pre_exposure=strata_pre_exposure,
                    **kwargs,
                )
                if strata_col
                else None
            )
            periods = (
                src.get_ab_test(
                    diff_t,
                    metrics,
                    period=period,
                    period_basis=period_basis,
                    period_population=period_population,
                    **kwargs,
                )
                if period
                else None
            )
    except Exception as exc:
        return {"error": str(exc)}
    messages = list(dict.fromkeys(str(w.message) for w in caught))
    from retentioneering.tools.ab_test import parse_metrics

    # the unit `reading` quotes: 0/1 metrics in percentage points (their delta and
    # interval fields are shares), every other metric in its own unit
    units = {
        spec.name: "pp" if spec.kind == "binary" else "value"
        for spec in parse_metrics(metrics)
    }
    scope_overall = _scope_text(correction, correction_scope, "this metric's row")
    scope_strata = _scope_text(
        correction, correction_scope, "this metric's strata rows"
    )
    scope_periods = _scope_text(
        correction, correction_scope, "this metric's period rows"
    )

    def _row(row: dict, columns: tuple) -> dict:
        out = {}
        for column in columns:
            value = row[column]
            if column.endswith("_paths") or column.endswith("_support"):
                out[column] = int(value)
            elif column in ("p_value", "p_value_adjusted", "stratum_vs_rest_p_value"):
                out[column] = _json_number(value, 8)
            elif isinstance(value, str):
                out[column] = value
            elif column in ("period_complete", "conditional"):
                out[column] = bool(value)
            else:
                out[column] = _json_number(value, 6)
        return out

    main_cols = (
        "population",
        "group1_paths",
        "group2_paths",
        "group1_support",
        "group2_support",
        "conditional",
        "group1_value",
        "group2_value",
        "delta",
        "delta_ci_low",
        "delta_ci_high",
        "rel_delta",
        "p_value",
        "p_value_adjusted",
        "mde",
        "verdict",
        "test",
        "correction_family",
        "mde_alpha",
        "support_warning",
        "reading",
    )
    entries = []
    for row in overall.to_dict("records"):
        unit = units.get(row["metric"], "value")
        entry = {
            "metric": row["metric"],
            "unit": unit,
            **_row(row, main_cols),
            "scope": scope_overall,
        }
        if strata is not None:
            part = strata[strata["metric"] == row["metric"]]
            top = part.reindex(part["delta"].abs().sort_values(ascending=False).index)
            entry["strata_count"] = int(len(part))
            entry["strata_selection"] = (
                f"top {min(3, len(part))} by |delta| of {len(part)}"
            )
            entry["heterogeneity_p_value"] = _json_number(
                part["heterogeneity_p_value"].iloc[0] if len(part) else None, 8
            )
            flag = part["strata_pre_exposure"].iloc[0] if len(part) else pd.NA
            entry["strata_correction_family"] = (
                str(part["correction_family"].iloc[0]) if len(part) else None
            )
            entry["strata_pre_exposure"] = None if pd.isna(flag) else bool(flag)
            entry["strata_pre_exposure_source"] = (
                str(part["strata_pre_exposure_source"].iloc[0]) if len(part) else None
            )
            entry["exploratory"] = bool(part["exploratory"].all())
            entry["strata"] = [
                _row(
                    r,
                    (
                        "stratum",
                        "group1_share",
                        "group2_share",
                        "delta",
                        "delta_ci_low",
                        "delta_ci_high",
                        "p_value",
                        "p_value_adjusted",
                        "mde_alpha",
                        "stratum_vs_rest_p_value",
                        "verdict",
                        "reading",
                    ),
                )
                | {"unit": unit, "scope": scope_strata}
                for r in top.head(3).to_dict("records")
            ]
        if periods is not None:
            part = periods[periods["metric"] == row["metric"]]
            entry["periods_correction_family"] = (
                str(part["correction_family"].iloc[0]) if len(part) else None
            )
            entry["periods"] = [
                _row(
                    r,
                    (
                        "period",
                        "period_complete",
                        "delta",
                        "delta_ci_low",
                        "delta_ci_high",
                        "p_value",
                        "p_value_adjusted",
                        "mde_alpha",
                        "verdict",
                        "reading",
                    ),
                )
                | {"unit": unit, "scope": scope_periods}
                for r in part.to_dict("records")
            ]
        entries.append(entry)
    out: dict[str, Any] = {
        "diff": list(diff),
        "path_col": path_col or src.schema.path_col,
        "ci_level": ci_level,
        "correction": correction,
        "correction_scope": correction_scope,
        "population": str(overall["population"].iloc[0]),
        "unassigned_paths": int(overall["unassigned_paths"].iloc[0]),
        "metrics": entries,
    }
    if horizon:
        out["horizon"] = horizon
        out["immature_paths"] = int(overall["immature_paths"].iloc[0])
    out["warnings"] = messages
    if strata_col:
        out["strata_col"] = strata_col
    if period:
        out["period"] = period
        out["period_basis"] = period_basis
        out["period_population"] = period_population
    return out


def get_ab_bootstrap(
    session: Any,
    diff: list,
    metrics: list,
    period: str,
    contrast: dict,
    period_basis: str = "calendar",
    horizon: str | None = None,
    strata_col: str | None = None,
    strata_pre_exposure: bool | None = None,
    ci_level: float = 0.95,
    n_bootstrap: int = 1000,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Contrast an A/B effect between periods with a joint bootstrap interval (novelty).

    Answers "is the early effect larger than the late one?" with one interval:
    every period is evaluated on the same resampled users, and both sides of a
    contrast run on the users observed in all its periods (no change of cohort
    mix). Numbers only (no report tab): quote them in backticks.

    Parameters
    ----------
    diff, metrics, path_col, local_preprocessors, horizon, strata_col,
    strata_pre_exposure:
        As in get_ab_test.
    period:
        The bins the contrast names: a fixed length with
        period_basis="since_assignment" ("14D" -> "0-13", "14-27", ...), or a
        calendar unit ("W").
    period_basis:
        "calendar" (default) or "since_assignment" (needs the session's assignment
        table with assigned_at; the novelty view).
    contrast:
        {"name": [periods_a, periods_b]}, e.g. {"novelty": [["0-13"], ["28-41"]]}:
        per metric, (value in a - value in b) within each arm, and delta = that
        change in group1 minus group2. Period labels as get_ab_test(period=...)
        returns them.
    ci_level:
        Level of the percentile interval (default 0.95).
    n_bootstrap:
        Replicates (default 1000, at least 100).

    Returns
    -------
    diff, period, period_basis, contrast, n_bootstrap, population, warnings and
    rows — one per metric (and stratum) and contrast: metric, [stratum],
    contrast, group1_value and group2_value (each arm's change between the sides),
    delta, delta_ci_low, delta_ci_high (percentile, unadjusted), p_value (from the
    bootstrap standard error, unadjusted), share_positive (share of replicates
    with delta > 0; not a p-value), valid_replicates, unit ("pp" or "value").
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    except Exception as exc:
        return {"error": f"local_preprocessors: {exc}"}
    from retentioneering.tools.ab_test import ABTest

    if not isinstance(contrast, dict) or not contrast:
        return {"error": "contrast must be {'name': [periods_a, periods_b]}"}
    sides = {
        name: tuple(value) if isinstance(value, list) else value
        for name, value in contrast.items()
    }
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            test = ABTest.from_stream(
                src,
                tuple(diff) if diff else None,
                metrics,
                assignment=getattr(session, "assignment", None),
                strata_col=strata_col or None,
                strata_pre_exposure=strata_pre_exposure,
                period=period,
                period_basis=period_basis,
                horizon=horizon or None,
                ci_level=ci_level,
                n_bootstrap=n_bootstrap,
                path_col=path_col or None,
            )
            table = test.contrast_summary(sides)
    except Exception as exc:
        return {"error": str(exc)}
    units = {s.name: "pp" if s.kind == "binary" else "value" for s in test.specs}
    rows = []
    for row in table.to_dict("records"):
        entry: dict[str, Any] = {"metric": row["metric"]}
        if "stratum" in row:
            entry["stratum"] = str(row["stratum"])
        entry["contrast"] = row["contrast"]
        for column in (
            "group1_value",
            "group2_value",
            "delta",
            "delta_ci_low",
            "delta_ci_high",
        ):
            entry[column] = _json_number(row[column], 6)
        entry["p_value"] = _json_number(row["p_value"], 8)
        entry["share_positive"] = _json_number(row["share_positive"], 4)
        entry["valid_replicates"] = int(row["valid_replicates"])
        entry["unit"] = units.get(row["metric"], "value")
        rows.append(entry)
    return {
        "diff": list(diff),
        "period": period,
        "period_basis": period_basis,
        "contrast": contrast,
        "n_bootstrap": test.resolved_n_bootstrap(),
        "population": "assignment" if test.matrix.with_assignment else "stream",
        "rows": rows,
        "warnings": list(dict.fromkeys(str(w.message) for w in caught)),
    }


def check_analysis(analysis: str) -> dict:
    """
    Validate analysis text BEFORE calling export_report.

    Scans for percentages (%) and multipliers (×) that have no [tab:ref] anchor
    link within 200 characters. Checks per number, not per line — a line with
    one link and one unlinked % will still be flagged for that %.
    Numbers in backtick spans `2.2×` are exempt (agent-computed values).

    Workflow (mandatory):
      1. Write your full analysis text.
      2. Call check_analysis(analysis).
      3. If status == "needs_fixes": fix EVERY listed line, then call check_analysis again.
      4. Only call export_report when check_analysis returns {"status": "ok"}.

    Returns
    -------
    {"status": "ok"}  — ready to export.
    {"status": "needs_fixes", "issues": [...]}  — lines to fix before exporting.
    """
    issues = _find_unlinked_numbers(analysis)
    if not issues:
        return {
            "status": "ok",
            "message": "All numbers have links. Proceed with export_report.",
        }
    return {
        "status": "needs_fixes",
        "count": len(issues),
        "issues": issues,
        "instruction": (
            "Add a [tab_label:element] anchor link to each listed line. "
            "For transitions use edge links [tab:src->tgt]. "
            "Then call check_analysis again until status is 'ok'."
        ),
    }


def export_report(
    session: Any,
    title: str = "Analysis Report",
    analysis: str | None = None,
) -> dict:
    """
    Finalize the report with all widgets added via add_transition_graph /
    add_step_matrix / add_segment_overview / add_ab_test. Needs at least one tab:
    numbers-only tools (get_ab_test, get_sample_ratio, get_conversion_rate)
    register none, so an A/B analysis adds its scorecard with add_ab_test.
    Resets the pending widget list afterwards.

    Parameters
    ----------
    title:
        Report title.
    analysis:
        Full written analysis in markdown. Reference specific tabs and events
        with [tab_label:event_name] — clicking the link will activate that tab
        and focus the event. Use [event_name] (no label prefix) to focus in
        whichever tab is currently active.
        Example:
          "Drop-off at [Overall Flow:basket] is 45%. Timing shows
           [Timing:basket] takes 2.3 min on average. In the funnel
           [Purchase Funnel:payment_details] is the main bottleneck."
        Supports markdown: # headings, **bold**, *italic*, tables, - lists.
    """
    if not session.pending_tabs:
        return {
            "error": (
                "No widgets added. Call an add_* tool first (add_transition_graph, "
                "add_step_matrix, add_segment_overview, or add_ab_test for an A/B "
                "analysis whose numbers came from numbers-only tools)."
            )
        }
    return session.package(title, analysis)
