"""
retentioneering MCP server.

Usage in a Jupyter notebook:
    from retentioneering.mcp import serve
    serve(stream)
    serve(stream, context={"description": "...", "events": {...}})
"""

from __future__ import annotations

import inspect
import json
import os
import pathlib
import re
import tempfile
import threading
from functools import wraps
from typing import Any

import pandas as pd
from mcp.server.fastmcp import FastMCP

from retentioneering._tracking import _caller_type as _tracking_caller_type
from retentioneering.eventstream.eventstream import Eventstream


def serve(
    stream: "Eventstream",
    context: dict | None = None,
    port: int = 8765,
) -> None:
    """
    Start a local MCP server exposing *stream* to Claude (or any MCP client).

    Parameters
    ----------
    stream:
        The prepared Eventstream to analyse.
    context:
        Optional semantic layer — descriptions of events, segments, KPIs, etc.
        Example::

            serve(stream, context={
                "description": "E-commerce store. Main KPI — purchase conversion.",
                "events": {"basket": "Added to cart", "purchase": "Completed purchase"},
            })
    port:
        HTTP port for the SSE transport (default 8765).

    Notes
    -----
    **Prompt caching (programmatic agents):** when calling the Anthropic API
    directly, add ``cache_control`` to the system message and tool definitions
    to cache the large static prefix::

        system=[{"type": "text", "text": instructions,
                 "cache_control": {"type": "ephemeral"}}]

    Claude Desktop handles caching automatically.
    """
    mcp = _build_server(stream, context or {}, port=port, notebook_dir=os.getcwd())
    thread = threading.Thread(
        target=lambda: mcp.run(transport="sse"),
        daemon=True,
    )
    thread.start()
    print(
        f"retentioneering MCP server running on port {port}.\n"
        f"Add to Claude Desktop config:\n"
        f'  "retentioneering": {{"url": "http://localhost:{port}/sse"}}'
    )


# ── server builder ─────────────────────────────────────────────────────────────


def _build_server(
    stream: "Eventstream",
    context: dict,
    port: int = 8765,
    notebook_dir: str = "",
) -> FastMCP:
    from retentioneering.widgets._html_export import write_report_html

    mcp = FastMCP(
        "retentioneering",
        instructions=_system_instructions(stream, context, notebook_dir=notebook_dir),
        port=port,
    )

    def _tool():
        """@mcp.tool() that sets caller='mcp' in the tracking context."""

        def decorator(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                token = _tracking_caller_type.set("mcp")
                try:
                    return fn(*args, **kwargs)
                finally:
                    _tracking_caller_type.reset(token)

            return mcp.tool()(wrapper)

        return decorator

    # Report builder state — accumulates widgets across add_* calls,
    # reset by export_report.
    _pending: list[dict] = []

    # Active stream — starts as the original, replaced by update_base_stream.
    # Use a list so nested functions can rebind it.
    _active: list = [stream]

    # Events flagged as important in context — prioritised in compact summaries.
    _context_events: set = set(context.get("events", {}).keys())

    # Preprocessors applied via update_base_stream (for data notes in report).
    _base_preprocessors: list = []

    @_tool()
    def update_base_stream(preprocessors: list) -> str:
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
            rename_events, edit_events, add_events, add_segment, drop_segment, add_clusters,
            url_events, sample_paths, split_sessions.
            Call describe_tool("<type>") for full parameter reference before using any step.

        Returns
        -------
        JSON with updated stream stats: n_paths, n_events_total, events list.
        """
        try:
            new_stream = _apply_preprocessors(stream, preprocessors)
        except Exception as exc:
            return json.dumps({"error": str(exc)})
        _active[0] = new_stream
        _base_preprocessors.clear()
        _base_preprocessors.extend(preprocessors)
        s = new_stream.schema
        df = new_stream.df
        return json.dumps(
            {
                "status": "base stream updated",
                "n_paths": int(df[s.path_col].nunique()),
                "n_events_total": len(df),
                "events": sorted(df[s.event_col].astype(str).unique().tolist()),
            },
            ensure_ascii=False,
        )

    @_tool()
    def reset_base_stream() -> str:
        """
        Reset the active stream to the original eventstream passed to serve().
        Use this when the user wants to start a new analysis branch from scratch
        or undo all preprocessing.

        Returns updated stream stats.
        """
        _active[0] = stream
        _base_preprocessors.clear()
        s = stream.schema
        df = stream.df
        return json.dumps(
            {
                "status": "base stream reset to original",
                "n_paths": int(df[s.path_col].nunique()),
                "n_events_total": len(df),
                "events": sorted(df[s.event_col].astype(str).unique().tolist()),
            },
            ensure_ascii=False,
        )

    @_tool()
    def playbook(scenario: str = "") -> str:
        """
        Return the canonical recipe for a named analysis scenario.
        Call this when you recognise a pattern in the user's question but need
        the step-by-step procedure. Do NOT improvise — look it up here first.

        Pass "" (empty) to list all available scenarios.
        """
        return json.dumps(
            _PLAYBOOK.get(scenario.strip(), _playbook_index()), ensure_ascii=False
        )

    @_tool()
    def describe_tool(tool: str = "") -> str:
        """
        Return full parameter documentation for a preprocessor type or internal tool.
        Call this before using any preprocessor you are not 100% sure about —
        do NOT guess parameters or read source files.

        Pass "" (empty) to list all documented tools.

        Preprocessors (use as {"type": "<name>", ...} in update_base_stream / local_preprocessors):
          collapse_events  filter_paths   filter_events   truncate_paths
          rename_events    edit_events    add_events      add_segment
          drop_segment     add_clusters   url_events      sample_paths
          split_sessions

        Reference topics:
          report_links   (anchor link syntax for analysis text)
        """
        t = tool.strip()
        if not t:
            return json.dumps(_tool_docs_index(), ensure_ascii=False)
        if t in _STATIC_TOOL_DOCS:
            return json.dumps(
                {"topic": t, "docs": _STATIC_TOOL_DOCS[t]}, ensure_ascii=False
            )
        method = getattr(Eventstream, t, None)
        doc = inspect.getdoc(method) if method and callable(method) else None
        if doc:
            return json.dumps({"preprocessor": t, "docs": doc}, ensure_ascii=False)
        return json.dumps(
            {"error": f"Unknown tool {t!r}.", **_tool_docs_index()}, ensure_ascii=False
        )

    @_tool()
    def describe() -> str:
        """Return schema, event list, unique path counts and available segments.
        Reflects the current active stream (after any update_base_stream calls)."""
        cur = _active[0]
        s = cur.schema
        ec, pc = s.event_col, s.path_col
        df = cur.df
        ts_col = s.timestamp
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
        if context.get("events"):
            result["event_descriptions"] = context["events"]
        if context.get("segments"):
            result["segment_descriptions"] = context["segments"]
        return json.dumps(result, ensure_ascii=False)

    @_tool()
    def add_transition_graph(
        label: str,
        edge_weight: str = "proba_out",
        diff: list | None = None,
        path_id_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a transition graph and register it as a tab in the pending report.
        Returns the transition matrix so you can analyse it before writing the report.

        Call this (possibly multiple times with different parameters) before
        calling export_report().

        Parameters
        ----------
        label:
            Tab label shown in the report. Use a short descriptive name,
            e.g. "Overall Flow" or "Mobile vs Desktop". No colons in the label.
        edge_weight:
            How to weight edges. One of: proba_out, proba_in, count, unique_paths,
            transition_rate, per_path, time_median, time_q95.
        diff:
            Optional diff: [segment_col, value1, value2].
        path_id_col:
            Override the path ID column.

        Returns
        -------
        JSON with tab_id, events list, values matrix (and group1/group2 in diff mode).
        Use tab_id to reference this tab in the analysis text as [label:event].

        local_preprocessors:
            Optional one-off preprocessing applied on top of the base stream for
            this visualisation only. Same format as update_base_stream preprocessors.
            Use sparingly — prefer update_base_stream when the same preprocessing
            applies to multiple visualisations.
        """
        src = _apply_preprocessors(_active[0], local_preprocessors or [])
        widget = src.transition_graph(
            edge_weight=edge_weight, diff=diff, path_id_col=path_id_col or None
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
        data = {
            "widget_type": "transition_graph",
            "result": json.loads(widget.result or "{}"),
            "edge_weight": widget.edge_weight,
            "diff": json.loads(widget.diff) if widget.diff else None,
            "event_counts": json.loads(widget.event_counts or "{}"),
            "event_counts_g1": json.loads(widget.event_counts_g1 or "{}"),
            "event_counts_g2": json.loads(widget.event_counts_g2 or "{}"),
            "node_positions": {},
            "event_visibility": {},
            "segment_levels": json.loads(widget.segment_levels or "{}"),
            "path_cols": json.loads(widget.path_cols or "[]"),
            "path_id_col": widget.path_id_col or "",
            "height": widget.height,
            "sidebar_open": False,
        }
        tab_id = f"tab-{len(_pending)}"
        _pending.append(
            {
                "label": label,
                "data": data,
                "local_preprocessors": local_preprocessors or [],
            }
        )

        result_raw = json.loads(widget.result or "{}")
        summary = _transition_graph_summary(result_raw, _context_events, edge_weight)
        summary["tab_id"] = tab_id
        summary["label"] = label
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def add_step_matrix(
        label: str,
        max_steps: int = 10,
        diff: list | None = None,
        path_pattern: str | None = None,
        path_id_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a step matrix and register it as a tab in the pending report.
        Returns the matrix data so you can analyse it before writing the report.

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
            Filter paths, e.g. "add_to_cart->.*->purchase".
        path_id_col:
            Override the path ID column.

        Returns
        -------
        JSON with tab_id, matrices list, event_counts.

        local_preprocessors: same as in add_transition_graph.
        """
        src = _apply_preprocessors(_active[0], local_preprocessors or [])
        widget = src.step_matrix(
            max_steps=max_steps,
            diff=diff,
            path_id_col=path_id_col or None,
            path_pattern=path_pattern or None,
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
        data = {
            "widget_type": "step_matrix",
            "result": json.loads(widget.result or "{}"),
            "max_steps": widget.max_steps,
            "diff": json.loads(widget.diff) if widget.diff else None,
            "path_id_col": widget.path_id_col or "",
            "path_pattern": widget.path_pattern or "",
            "path_cols": json.loads(widget.path_cols or "[]"),
            "segment_levels": json.loads(widget.segment_levels or "{}"),
            "event_list": json.loads(widget.event_list or "[]"),
            "height": widget.height,
            "sidebar_open": False,
            "display_prefs": "{}",
        }
        tab_id = f"tab-{len(_pending)}"
        _pending.append(
            {
                "label": label,
                "data": data,
                "local_preprocessors": local_preprocessors or [],
            }
        )

        result_raw = json.loads(widget.result or "{}")
        summary = _step_matrix_summary(result_raw, _context_events)
        summary["tab_id"] = tab_id
        summary["label"] = label
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def add_segment_overview(
        label: str,
        segment_col: str,
        metrics_config: list | None = None,
        path_id_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a segment overview and register it as a tab in the pending report.
        Returns the overview table so you can analyse it before writing the report.

        Call this (possibly multiple times with different segment columns) before
        calling export_report().

        Parameters
        ----------
        label:
            Tab label shown in the report. No colons in the label.
        segment_col:
            Column to segment by (must be listed in segment_cols from describe()).
        metrics_config:
            List of additional metric dicts. segment_size and segment_share are
            always computed automatically and do not need to be specified.

            Each dict: {"metric": <name>, "metric_args": {...}, "agg": <agg>}
            "agg" choices: "mean" (default), "median", "complement_diff",
                           "q5", "q25", "q75", "q95"

            Available metrics and their metric_args:
              {"metric": "length"}
                  — number of events per path
              {"metric": "duration"}
                  — duration in seconds (first to last event)
              {"metric": "event_count", "metric_args": {"events": "purchase"}}
                  — how many times the event occurred; events can also be a list
              {"metric": "has", "metric_args": {"events": "purchase"}}
                  — 0/1 whether the path contains the event (conversion rate)
              {"metric": "time_between",
               "metric_args": {"event_from": "add_to_cart", "event_to": "purchase"}}
                  — seconds between first occurrences of two events
              {"metric": "matches",
               "metric_args": {"pattern": "add_to_cart->.*->purchase"}}
                  — 0/1 whether path matches the pattern

            Examples:
              Conversion rate to purchase by platform:
                metrics_config=[
                  {"metric": "has",  "metric_args": {"events": "purchase"}, "agg": "mean"},
                  {"metric": "length"},
                  {"metric": "duration", "agg": "median"},
                ]
              Time-to-purchase by acquisition channel:
                metrics_config=[
                  {"metric": "time_between",
                   "metric_args": {"event_from": "home", "event_to": "purchase"},
                   "agg": "median"},
                ]
        path_id_col:
            Override the path ID column.

        Returns
        -------
        JSON with tab_id, metrics list, segments list, values matrix.

        local_preprocessors: same as in add_transition_graph.
        """
        src = _apply_preprocessors(_active[0], local_preprocessors or [])
        widget = src.segment_overview(
            segment_col=segment_col,
            metrics_config=metrics_config or [],
            path_id_col=path_id_col or None,
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
        data = {
            "widget_type": "segment_overview",
            "result": json.loads(widget.result or "{}"),
            "segment_col": widget.segment_col or "",
            "path_id_col": widget.path_id_col or "",
            "metrics_config": json.loads(widget.metrics_config or "[]"),
            "segment_cols": json.loads(widget.segment_cols or "[]"),
            "segment_levels": json.loads(widget.segment_levels or "{}"),
            "path_cols": json.loads(widget.path_cols or "[]"),
            "event_list": json.loads(widget.event_list or "[]"),
            "height": widget.height,
            "sidebar_open": False,
        }
        tab_id = f"tab-{len(_pending)}"
        _pending.append(
            {
                "label": label,
                "data": data,
                "local_preprocessors": local_preprocessors or [],
            }
        )

        result_raw = json.loads(widget.result or "{}")
        summary = _segment_overview_summary(result_raw, _context_events)
        summary["tab_id"] = tab_id
        summary["label"] = label
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def check_analysis(analysis: str) -> str:
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
            return json.dumps(
                {
                    "status": "ok",
                    "message": "All numbers have links. Proceed with export_report.",
                },
                ensure_ascii=False,
            )
        return json.dumps(
            {
                "status": "needs_fixes",
                "count": len(issues),
                "issues": issues,
                "instruction": (
                    "Add a [tab_label:element] anchor link to each listed line. "
                    "For transitions use edge links [tab:src->tgt]. "
                    "Then call check_analysis again until status is 'ok'."
                ),
            },
            ensure_ascii=False,
        )

    @_tool()
    def export_report(
        title: str = "Analysis Report",
        analysis: str | None = None,
        path: str | None = None,
    ) -> str:
        """
        Generate the HTML report with all widgets added via add_transition_graph /
        add_step_matrix. Resets the pending widget list afterwards.

        Parameters
        ----------
        title:
            Report title shown in the browser tab and as the heading.
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
        path:
            Destination file path. If None, a temp file is created.
        """
        if not _pending:
            return json.dumps(
                {
                    "error": "No widgets added. Call add_transition_graph or add_step_matrix first."
                }
            )

        if path is None:
            tmp = tempfile.NamedTemporaryFile(
                suffix=".html", delete=False, prefix="retentioneering_"
            )
            path = tmp.name
            tmp.close()

        widgets = list(_pending)

        data_sources_html = _build_data_note(list(_base_preprocessors), widgets)
        write_report_html(
            path, title, widgets, analysis, data_sources_html=data_sources_html
        )
        _pending.clear()  # only clear after successful write
        return json.dumps(
            {
                "path": str(pathlib.Path(path).resolve()),
                "title": title,
                "tabs": [w["label"] for w in widgets],
            }
        )

    return mcp


# ── helpers ────────────────────────────────────────────────────────────────────


def _apply_preprocessors(stream: Any, preprocessors: list) -> Any:
    """Apply an ordered list of preprocessing steps to an Eventstream."""
    for step in preprocessors:
        t = step.get("type", "")
        if t == "collapse_events":
            stream = stream.collapse_events(
                repetitive=step.get("repetitive"),
                event_groups=step.get("event_groups"),
            )
        elif t == "filter_paths":
            condition = {
                k: v for k, v in step.items() if k not in ("type", "path_id_col")
            }
            stream = stream.filter_paths(condition, path_id_col=step.get("path_id_col"))
        elif t == "filter_events":
            sql = step.get("sql")
            by_col = {k: v for k, v in step.items() if k not in ("type", "sql")}
            stream = stream.filter_events(by_column=by_col or None, sql=sql)
        elif t == "truncate_paths":
            stream = stream.truncate_paths(
                left=step["left"],
                right=step["right"],
                path_id_col=step.get("path_id_col"),
            )
        elif t == "rename_events":
            stream = stream.rename_events(mapping=step["mapping"])
        elif t == "edit_events":
            stream = stream.edit_events(
                rename=step.get("rename"),
                delete=step.get("delete"),
            )
        elif t == "add_events":
            stream = stream.add_events(
                new_event_name=step["new_event_name"],
                source_events=step.get("source_events"),
                sql=step.get("sql"),
                churn=step.get("churn"),
            )
        elif t == "daily_states":
            stream = stream.daily_states(
                active_events=step.get("active_events"),
                max_dormant_days=step.get("max_dormant_days", 30),
            )
        elif t == "add_segment":
            stream = stream.add_segment(
                name=step["name"],
                values=step.get("values"),
                sql=step.get("sql"),
                funnel_events=step.get("funnel_events"),
                path_id_col=step.get("path_id_col"),
            )
        elif t == "drop_segment":
            stream = stream.drop_segment(name=step["name"])
        elif t == "add_clusters":
            stream = stream.add_clusters(
                segment_name=step["segment_name"],
                features=step["features"],
                method=step.get("method", "kmeans"),
                n_clusters=step.get("n_clusters"),
                path_id_col=step.get("path_id_col"),
            )
        elif t == "url_events":
            stream = stream.url_events(
                column=step["column"],
                nodes=step["nodes"],
                strip_host=step.get("strip_host", True),
                strip_cgi=step.get("strip_cgi", True),
            )
        elif t == "sample_paths":
            stream = stream.sample_paths(
                sample_size=step["sample_size"],
                random_state=step.get("random_state"),
            )
        elif t == "split_sessions":
            stream = stream.split_sessions(
                timeout=step.get("timeout"),
                session_col=step.get("session_col", "session_id"),
                session_index_col=step.get("session_index_col", "session_index"),
            )
        else:
            raise ValueError(
                f"Unknown preprocessor type: {t!r}. "
                "Supported: collapse_events, filter_paths, filter_events, "
                "truncate_paths, rename_events, edit_events, add_events, "
                "add_segment, drop_segment, add_clusters, url_events, "
                "sample_paths, split_sessions."
            )
    return stream


def _transition_graph_summary(
    result_raw: dict, context_events: set, edge_weight: str, top_n: int = 25
) -> dict:
    """Compact transition graph summary: top-N edges, context events prioritised."""
    events = result_raw.get("events", [])
    values = result_raw.get("values", [])
    is_diff = bool(result_raw.get("group1"))
    n = len(events)
    if n == 0 or not values:
        return {"n_events": 0, "top_edges": []}

    edges: list[tuple] = []
    for i, src in enumerate(events):
        for j, tgt in enumerate(events):
            try:
                w = values[i][j]
            except IndexError:
                continue
            if w is not None and w != 0:
                priority = src in context_events or tgt in context_events
                edges.append((src, tgt, w, priority))

    if is_diff:
        # Separate top increases / decreases; enrich with per-group values
        g1 = result_raw.get("group1", {})
        g2 = result_raw.get("group2", {})
        g1_ev, g1_val = g1.get("events", events), g1.get("values", [])
        g2_ev, g2_val = g2.get("events", events), g2.get("values", [])

        def _gval(ev_list: list, val_matrix: list, src: str, tgt: str):
            try:
                return round(val_matrix[ev_list.index(src)][ev_list.index(tgt)] or 0, 4)
            except (ValueError, IndexError):
                return None

        def _fmt(e: tuple) -> dict:
            d: dict = {"from": e[0], "to": e[1], "diff": round(e[2], 4)}
            g1v = _gval(g1_ev, g1_val, e[0], e[1])
            g2v = _gval(g2_ev, g2_val, e[0], e[1])
            if g1v is not None:
                d["g1"] = g1v
            if g2v is not None:
                d["g2"] = g2v
            return d

        pos = sorted([e for e in edges if e[2] > 0], key=lambda e: (not e[3], -e[2]))[
            : top_n // 2
        ]
        neg = sorted([e for e in edges if e[2] < 0], key=lambda e: (not e[3], e[2]))[
            : top_n // 2
        ]
        return {
            "n_events": n,
            "top_increases": [_fmt(e) for e in pos],
            "top_decreases": [_fmt(e) for e in neg],
            "note": f"Top {len(pos) + len(neg)} diff edges of {len(edges)} non-zero. Full graph in tab.",
        }
    else:
        shown = sorted(edges, key=lambda e: (not e[3], -abs(e[2])))[:top_n]
        return {
            "n_events": n,
            "edge_weight": edge_weight,
            "top_edges": [
                {"from": s, "to": t, "weight": round(w, 4)} for s, t, w, _ in shown
            ],
            "note": f"Top {len(shown)} of {len(edges)} non-zero edges. Full graph in tab.",
        }


def _step_matrix_summary(
    result_raw: dict, context_events: set, top_per_step: int = 5
) -> dict:
    """Compact step matrix: top events per step + full rows for context events."""
    matrices = result_raw.get("matrices", [])
    if not matrices:
        return {"note": "no data"}

    m = matrices[0]
    events = m.get("events", [])
    columns = m.get("columns", [])
    values = m.get("values", [])
    is_diff = bool(m.get("group1"))

    if not events or not columns:
        return {"n_events": 0}

    by_step: dict = {}
    for ci, col in enumerate(columns):
        step_vals: list = []
        for ri, ev in enumerate(events):
            try:
                v = values[ri][ci]
                if v is not None:
                    step_vals.append((abs(v), v, ev))
            except IndexError:
                pass
        step_vals.sort(reverse=True)
        by_step[str(col)] = [
            {"event": ev, "value": round(v, 4)} for _, v, ev in step_vals[:top_per_step]
        ]

    ctx_rows: dict = {}
    for ri, ev in enumerate(events):
        if ev in context_events and ri < len(values):
            row = {
                str(col): round(values[ri][ci], 4)
                for ci, col in enumerate(columns)
                if ri < len(values)
                and ci < len(values[ri])
                and values[ri][ci] is not None
            }
            if row:
                ctx_rows[ev] = row

    result: dict = {
        "n_events": len(events),
        "steps": columns,
        "top_events_per_step": by_step,
    }
    if ctx_rows:
        result["context_event_rows"] = ctx_rows
    note = f"Top {top_per_step} events per step shown."
    if is_diff:
        note += " Values are differences (g2 − g1)."
    result["note"] = note + " Full matrix in tab."
    return result


def _segment_overview_summary(result_raw: dict, context_events: set) -> dict:
    """Compact segment overview: full metrics table + highlight rows with large spread."""
    metrics = result_raw.get("metrics", [])
    segments = result_raw.get("segments", [])
    values = result_raw.get("values", [])

    if not metrics or not segments or not values:
        return {"note": "no data"}

    # Attach spread (max − min) to each metric row so agent knows what's interesting
    rows = []
    for mi, metric in enumerate(metrics):
        if mi >= len(values):
            continue
        row_vals = [v for v in values[mi] if v is not None]
        spread = round(max(row_vals) - min(row_vals), 4) if len(row_vals) >= 2 else 0.0
        row: dict = {"metric": metric, "spread": spread}
        for si, seg in enumerate(segments):
            if si < len(values[mi]) and values[mi][si] is not None:
                row[seg] = round(values[mi][si], 4)
        # Flag if this metric involves a context event
        is_context = any(ev in metric for ev in context_events)
        row["_context"] = is_context
        rows.append(row)

    # Sort: context rows first, then by spread descending
    rows.sort(key=lambda r: (not r.pop("_context"), -r["spread"]))

    return {
        "segments": segments,
        "metrics_table": rows,
        "note": "Rows sorted by relevance (context events first, then by spread across segments).",
    }


def _df_to_matrix(df: Any) -> dict:
    return {
        "events": df.index.tolist(),
        "columns": [int(c) for c in df.columns.tolist()],
        "values": df.values.tolist(),
    }


def _df_to_list(df: Any) -> list:
    rows = []
    for _, row in df.iterrows():
        cells = []
        for v in row:
            if pd.isna(v):
                cells.append(None)
            elif isinstance(v, pd.Timedelta):
                cells.append(v.total_seconds())
            elif hasattr(v, "__float__"):
                cells.append(float(v))
            else:
                cells.append(v)
        rows.append(cells)
    return rows


def _step_to_code(step: dict) -> str:
    """Convert a preprocessor step dict to its Python Eventstream call."""
    t = step.get("type", "")
    args = {k: v for k, v in step.items() if k != "type"}
    kw = ", ".join(f"{k}={v!r}" for k, v in args.items())

    if t == "collapse_events":
        if args.get("repetitive"):
            return "stream.collapse_events(repetitive=True)"
        elif args.get("event_groups"):
            return f"stream.collapse_events(event_groups={args['event_groups']!r})"
    elif t == "filter_events":
        return f"stream.filter_events(by_column={args!r})"
    elif t == "filter_paths":
        return f"stream.filter_paths({args!r})"
    elif t == "add_segment":
        if args.get("funnel_events"):
            return f"stream.add_segment(name={args['name']!r}, funnel_events={args['funnel_events']!r})"
        elif args.get("values"):
            return (
                f"stream.add_segment(name={args['name']!r}, values={args['values']!r})"
            )
        elif args.get("sql"):
            return f"stream.add_segment(name={args['name']!r}, sql={args['sql']!r})"
    return f"stream.{t}({kw})" if kw else f"stream.{t}()"


def _find_unlinked_numbers(analysis: str) -> list[dict]:
    """
    Find percentages/multipliers not covered by a nearby [link] reference.

    Rules:
    - A number is OK if a [tab:ref] link appears within 200 chars on the same line.
    - Numbers inside backtick code spans `like 2.2×` are exempt (agent-computed).
    - Lines inside fenced code blocks are skipped.
    - Horizontal rules (---) are skipped.

    Returns a list of {line, number, text} dicts — one entry per problematic number.
    """
    issues: list[dict] = []
    in_code_fence = False

    _EDGE_PAT = re.compile(r"(\w[\w_]*)(?:\s*(?:→|->)\s*)(\w[\w_]*)")

    for lineno, raw in enumerate(analysis.split("\n"), 1):
        s = raw.strip()
        if s.startswith("```"):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence or not s or re.match(r"^-{3,}$", s):
            continue

        # Flag edges written as code spans — e.g. `review_order → purchase`
        # Edges must ALWAYS be [tab:src->tgt] links, never backtick code spans.
        for cm in re.finditer(r"`([^`]*)`", s):
            span = cm.group(1)
            if _EDGE_PAT.search(span):
                issues.append(
                    {
                        "line": lineno,
                        "number": cm.group(),
                        "text": s[:120] + ("…" if len(s) > 120 else ""),
                        "hint": "Edge in code span — use [tab:src->tgt] link instead.",
                    }
                )

        # Remove inline code spans before the remaining checks
        s_no_code = re.sub(r"`[^`]*`", "", s)

        # Find all link positions
        link_positions = [m.start() for m in re.finditer(r"\[[^\]]+\]", s_no_code)]

        # Flag edge notation not inside a link
        for m in _EDGE_PAT.finditer(s_no_code):
            pos = m.start()
            # Check it's not already inside a [tab:src->tgt] link
            in_link = any(
                lm.start() <= pos <= lm.end()
                for lm in re.finditer(r"\[[^\]]+\]", s_no_code)
            )
            if not in_link:
                issues.append(
                    {
                        "line": lineno,
                        "number": m.group(),
                        "text": s[:120] + ("…" if len(s) > 120 else ""),
                        "hint": "Edge in plain text — use [tab:src->tgt] link.",
                    }
                )

        # Check each percentage / multiplier individually
        for m in re.finditer(
            r"\d+[.,]?\d*\s*%|\d+[.,]?\d*\s*×|×\s*\d+\.?\d*", s_no_code
        ):
            pos = m.start()
            nearby = any(abs(lp - pos) <= 200 for lp in link_positions)
            if not nearby:
                issues.append(
                    {
                        "line": lineno,
                        "number": m.group(),
                        "text": s[:120] + ("…" if len(s) > 120 else ""),
                    }
                )

    return issues


def _build_data_note(base_preprocessors: list, widgets: list) -> str:
    """
    Build an HTML <details> block showing the Python code that produced each tab.
    Injected above the analysis text in the report.
    """
    sections: list[str] = []

    if base_preprocessors:
        lines = "\n".join(_step_to_code(s) for s in base_preprocessors)
        sections.append(f"# Applied to all tabs\n{lines}")

    for w in widgets:
        lp = w.get("local_preprocessors", [])
        if lp:
            lines = "\n".join(_step_to_code(s) for s in lp)
            sections.append(f"# Tab: {w['label']}\n{lines}")

    if not sections:
        return ""

    code_block = "\n\n".join(sections)
    return (
        '<details style="margin-bottom:16px">'
        '<summary style="cursor:pointer;font-size:13px;color:#6b7280;user-select:none">'
        "Data sources — click to expand"
        "</summary>"
        f'<pre style="margin-top:8px;padding:12px;background:#f8fafc;border:1px solid #e5e7eb;'
        f'border-radius:6px;font-size:12px;overflow-x:auto;white-space:pre-wrap">'
        f"{code_block}"
        "</pre></details>"
    )


# ── playbook / describe_tool ───────────────────────────────────────────────────


def _load_playbook() -> dict[str, str]:
    """Parse playbook.md into {scenario_key: content_string}."""
    md = (pathlib.Path(__file__).parent / "playbook.md").read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    current_key: str | None = None
    buf: list[str] = []
    for line in md.splitlines():
        if line.startswith("## "):
            if current_key:
                sections[current_key] = "\n".join(buf).strip()
            current_key = line[3:].strip()
            buf = []
        elif not line.startswith("<!--"):
            buf.append(line)
    if current_key:
        sections[current_key] = "\n".join(buf).strip()
    return sections


_PLAYBOOK: dict[str, str] = _load_playbook()

# Static reference topics that don't map to an Eventstream method
_STATIC_TOOL_DOCS: dict[str, str] = {
    "report_links": """\
Anchor link syntax for the analysis text passed to export_report().

  [Tab:event]           open tab, focus node in transition graph
  [Tab:src->tgt]        open tab, animate edge (marching ants)
  [Tab:event@step]      open tab, scroll to step-matrix cell; step_window expands automatically
  [Tab:metric@segment]  open tab, highlight segment overview cell
  [Tab:segment_value]   open tab, highlight segment overview column
  [Tab:]                open tab only, no focus
  [Tab Name]            bare tab name — same as [Tab Name:]
  [event]               focus event/edge in the currently active tab

Rule: every cited number must be followed by an anchor link to its source.
      Use edge links [Tab:src->tgt] for transition percentages — NEVER node-only links.

Examples:
  '**[Flow:add_to_cart->purchase]** reaches 38% conversion'
  'Mobile shows **12%** [Segments:mobile], desktop **31%** [Segments:desktop]'
  '67% of sessions exit at **[Funnel:checkout@4]**'
  'See [Overview:] for the full KPI comparison'""",
}


def _playbook_index() -> dict:
    return {
        "usage": "Call playbook(scenario) with one of the keys below.",
        "scenarios": sorted(_PLAYBOOK.keys()),
    }


def _tool_docs_index() -> dict:
    preprocessors = sorted(
        m
        for m in dir(Eventstream)
        if not m.startswith("_")
        and callable(getattr(Eventstream, m))
        and inspect.getdoc(getattr(Eventstream, m))
        and m
        in {
            "filter_events",
            "filter_paths",
            "add_segment",
            "collapse_events",
            "truncate_paths",
            "rename_events",
            "edit_events",
            "add_events",
            "drop_segment",
            "add_clusters",
            "url_events",
            "sample_paths",
            "split_sessions",
        }
    )
    return {
        "usage": "Call describe_tool(topic) with one of the keys below.",
        "preprocessors": preprocessors,
        "reference_topics": sorted(_STATIC_TOOL_DOCS.keys()),
    }


def _system_instructions(
    stream: "Eventstream", context: dict, notebook_dir: str = ""
) -> str:
    s = stream.schema
    df = stream.df
    n_paths = int(df[s.path_col].nunique())
    events = sorted(df[s.event_col].astype(str).unique().tolist())

    lines = [
        "You are a product analytics assistant with access to a user behaviour eventstream.",
        f"The stream contains {n_paths} unique paths and {len(events)} distinct events.",
        f"Event column: '{s.event_col}'. Path column: '{s.path_col}'.",
    ]
    if context.get("description"):
        lines.append(f"Business context: {context['description']}")
    if context.get("events"):
        descs = ", ".join(f"'{k}': {v}" for k, v in context["events"].items())
        lines.append(f"Event meanings: {descs}")
    if context.get("kpis"):
        descs = ", ".join(f"'{k}': {v}" for k, v in context["kpis"].items())
        lines.append(f"Key metrics: {descs}")
    lines += [
        "",
        "## Workflow",
        "",
        "IMPORTANT — before doing anything else, classify the user's question:",
        "  A) Mentions a specific date range or time window?",
        "     → Apply the TEMPORAL ANOMALY pattern (see ## Analysis patterns below).",
        "     → This REQUIRES calling update_base_stream with add_segment(values=[...]).",
        "     → Do NOT skip this step and jump straight to add_transition_graph.",
        "  B) Asks why conversion from A to B is dropping?",
        "     → Apply the CONVERSION DROP-OFF pattern (funnel_events).",
        "  C) General exploration or comparison between segments?",
        "     → Follow the standard workflow below.",
        "",
        "1. Call describe() to understand the data (always first).",
        "2. Optionally call update_base_stream(preprocessors) to filter/transform the stream",
        "   for the entire analysis session (e.g. remove noise events, filter short paths).",
        "   Ask the user for confirmation before calling — it resets the analysis context.",
        "   All subsequent add_* calls use this preprocessed stream automatically.",
        "3. Call add_transition_graph(), add_step_matrix(), and/or add_segment_overview().",
        "   Each call returns a compact summary for analysis and registers a tab in the report.",
        "   The full interactive visualisation is always embedded in the HTML report.",
        "   PARALLELISM: add_* calls are independent — if your client supports parallel",
        "   tool calls, issue them simultaneously to save time.",
        "4. Write your analysis text (see ## Analysis text below).",
        "   Numbers you computed yourself (not read from a tab) → wrap in backticks: `2.2×`.",
        "5. Call check_analysis(analysis) — MANDATORY before export_report.",
        "   Fix every issue it returns. Repeat until status is 'ok'.",
        "6. Call export_report(analysis=<validated_text>).",
        "   The HTML file will contain all added tabs and the analysis panel.",
        "",
        "## Analysis text",
        "- Use markdown: # Heading, ## Sub, **bold**, *italic*, - list, | table |.",
        "- Reference widgets using [tab_label:ref] syntax:",
        "    [Overall Flow:basket]               → focus node 'basket' in transition graph",
        "    [Overall Flow:basket->purchase]      → animate edge basket→purchase (marching ants)",
        "    [Purchase Funnel:basket@4]           → scroll to cell basket at step 4 in step matrix",
        "                                           (step_window expands automatically if needed)",
        "    [Platform Breakdown:mobile]            → highlight 'mobile' column in segment_overview",
        "                                             (displayed as 'platform: mobile' in report)",
        "    [Platform Breakdown:has_purchase@mobile] → highlight specific cell: metric@segment",
        "    [basket]                             → focus in whichever tab is currently active",
        "- Tab labels must not contain colons.",
        "- Numbers you derived yourself (not read from a tab): wrap in backticks `2.2×` — exempt from link requirement.",
        "  EXCEPTION: backticks are ONLY for plain numbers/%. NEVER use backticks for edges (A → B).",
        "  Edges (transition A → B) must ALWAYS be [tab:A->B] links — never plain text, never backticks.",
        "- MANDATORY: EVERY number/% READ from a tab MUST have an anchor link.",
        "  Numbers without links are FORBIDDEN — in prose, tables, and lists alike.",
        "- When mentioning a transition between two events — anywhere in the text — ALWAYS use",
        "  an edge link [tab:src->tgt]. NEVER write 'A → B' as plain text. Examples:",
        "    CORRECT: '**[Flow:review_order->purchase]** — **38%** в spike'",
        "    CORRECT: | **[Flow:review_order->purchase]** | **38%** | **22%** |",
        "    WRONG: 'review_order → purchase: 38%'   ← no link",
        "    WRONG: '[Flow:review_order]: 38%'        ← node link instead of edge",
        "- Segment KPI → use column or cell link:",
        "    '**[Segments:mobile]**: **12%** vs **[Segments:desktop]**: **31%**'",
        "- Tab reference without specific element → bare tab name or [Tab:]:",
        "    '**[KPI по периодам]** показывает ...'   OR   '**[KPI по периодам:]**'",
        "- Always call export_report() and tell the user the file path.",
        "",
        "## Preprocessing",
        "- update_base_stream(preprocessors): changes the baseline for the whole session.",
        "  Use when the user says 'filter X and analyse', 'remove noise', 'work with Y'.",
        "  ALWAYS ask for confirmation first: 'This will replace the current stream. Proceed?'",
        "  After confirmation, call it ONCE — all add_* tools will use the new stream.",
        "  Calling it again starts a new analysis branch from the original stream.",
        "- local_preprocessors in add_*: one-off preprocessing for a single visualisation.",
        "  Use only when the preprocessing is specific to that visualisation and would NOT",
        "  make sense applied globally (e.g. 'show a graph with only mobile users' while",
        "  the rest of the analysis remains on all users).",
        "  Do NOT repeat the same local_preprocessors across multiple add_* calls —",
        "  use update_base_stream instead.",
        "",
        "## Analysis patterns",
        "",
        "### Temporal anomaly — spike, drop, incident, unusual period",
        "Trigger: user asks about a specific date range ('why did X spike on Jan 20-22?',",
        "  'what happened during those dates?', etc.).",
        "Do NOT execute Python code. Call update_base_stream (ask user to confirm) with:",
        "  preprocessors=[{",
        "    'type': 'add_segment', 'name': 'period',",
        "    'values': [",
        "      ['{timestamp_col}', '<',  'YYYY-MM-DD', 'normal'],",
        "      ['{timestamp_col}', '>',  'YYYY-MM-DD', 'normal'],",
        "      ['anomaly']",
        "    ]",
        "  }]",
        "  Replace {timestamp_col} with the value from describe() ('timestamp_col' field).",
        "  The first date is the start of the anomaly window, the second is the end.",
        "  Values format: each entry is [column, op, value, segment_label];",
        "  the last entry ['anomaly'] is the ELSE fallback.",
        "Then use diff=['period', 'anomaly', 'normal'] in add_transition_graph / add_step_matrix.",
        "Also: add_segment_overview(segment_col='period') to compare all KPIs.",
        "",
        "### Segment comparison (e.g. new vs loyal, mobile vs desktop)",
        "The segment column is already in the stream — no preprocessing needed.",
        "Use diff=['segment_col', 'val1', 'val2'] directly in add_transition_graph / add_step_matrix.",
        "",
        "### Funnel / path analysis (conversion from event A to event B)",
        "Use add_step_matrix with path_pattern='A->.*->B' to focus on paths",
        "that pass through both anchor events.",
        "",
        "### Conversion drop-off analysis (why is conversion from A to B dropping?)",
        "Use add_segment with funnel_events to create an N+1 level funnel segment.",
        "Each level is named after the LAST funnel event reached (in sequential order).",
        "Example for a 2-step funnel [add_to_cart, purchase]:",
        "  preprocessors=[{",
        "    'type': 'add_segment', 'name': 'funnel',",
        "    'funnel_events': ['add_to_cart', 'purchase']",
        "  }]",
        "  → segment levels: 'out_of_funnel', 'add_to_cart', 'purchase'",
        "  Then: diff=['funnel', 'add_to_cart', 'purchase']  (compare dropped vs converted)",
        "For a 3-step funnel [add_to_cart, checkout, purchase]:",
        "  funnel_events: ['add_to_cart', 'checkout', 'purchase']",
        "  → levels: 'out_of_funnel', 'add_to_cart', 'checkout', 'purchase'",
        "  To analyse checkout→purchase drop-off: diff=['funnel', 'checkout', 'purchase']",
        "  To analyse add_to_cart→checkout drop-off: diff=['funnel', 'add_to_cart', 'checkout']",
        "",
        "### Noise removal before any analysis",
        "Call update_base_stream first (ask user to confirm) with one or more of:",
        "  collapse_events repetitive=True  — removes self-loops",
        "  filter_paths length > N          — removes very short sessions",
        "  filter_events column/values      — removes specific noise events",
        f"- Save reports to the notebook directory: {notebook_dir}"
        if notebook_dir
        else "- Save reports to a convenient local path.",
    ]
    return "\n".join(lines)
