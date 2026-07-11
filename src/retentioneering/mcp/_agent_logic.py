"""Transport-independent MCP agent logic: applying preprocessor steps,
building compact tab summaries, validating analysis text, and rendering the
report's "data sources" note. None of this touches FastMCP/SSE or session
state (see `_report_session.py` for that) — `server.py` is just a thin
adapter wiring these functions up as `@mcp.tool()`s.
"""

from __future__ import annotations

import re
from typing import Any


def _apply_preprocessors(stream: Any, preprocessors: list) -> Any:
    """Apply an ordered list of preprocessing steps to an Eventstream."""
    for step in preprocessors:
        t = step.get("type", "")
        if t == "collapse_events":
            stream = stream.collapse_events(
                consecutive=step.get("consecutive"),
                event_groups=step.get("event_groups"),
                group_col=step.get("group_col"),
                session_id_col=step.get("session_id_col"),
                session_type_col=step.get("session_type_col"),
                agg=step.get("agg"),
                path_col=step.get("path_col"),
            )
        elif t == "filter_paths":
            condition = {k: v for k, v in step.items() if k not in ("type", "path_col")}
            stream = stream.filter_paths(condition, path_col=step.get("path_col"))
        elif t == "filter_events":
            stream = stream.filter_events(
                keep=step.get("keep"),
                drop=step.get("drop"),
                sql=step.get("sql"),
            )
        elif t == "truncate_paths":
            stream = stream.truncate_paths(
                start_event=step["start_event"],
                end_event=step["end_event"],
                path_col=step.get("path_col"),
            )
        elif t == "rename_events":
            stream = stream.rename_events(mapping=step["mapping"])
        elif t == "edit_events":
            stream = stream.edit_events(
                rename=step.get("rename"),
                delete=step.get("delete"),
            )
        elif t == "drop_events":
            stream = stream.drop_events(names=step["names"])
        elif t == "add_events":
            stream = stream.add_events(
                name=step["name"],
                source_events=step.get("source_events"),
                sql=step.get("sql"),
                churn=step.get("churn"),
            )
        elif t == "to_daily_states":
            stream = stream.to_daily_states(
                active_events=step.get("active_events"),
                max_dormant_days=step.get("max_dormant_days", 30),
            )
        elif t == "add_segment":
            stream = stream.add_segment(
                name=step["name"],
                rules=step.get("rules"),
                sql=step.get("sql"),
                funnel_events=step.get("funnel_events"),
                path_col=step.get("path_col"),
            )
        elif t == "drop_segment":
            stream = stream.drop_segment(name=step["name"])
        elif t == "add_clusters":
            stream = stream.add_clusters(
                name=step["name"],
                features=step["features"],
                method=step.get("method", "kmeans"),
                scaler=step.get("scaler"),
                n_clusters=step.get("n_clusters"),
                min_cluster_size=step.get("min_cluster_size"),
                cluster_selection_epsilon=step.get("cluster_selection_epsilon"),
                nmf_components=step.get("nmf_components"),
                path_col=step.get("path_col"),
            )
        elif t == "urls_to_events":
            stream = stream.urls_to_events(
                column=step["column"],
                nodes=step["nodes"],
                strip_host=step.get("strip_host", True),
                strip_query=step.get("strip_query", True),
                strip_locale=step.get("strip_locale", True),
                keep_full_paths=step.get("keep_full_paths", False),
                host_col=step.get("host_col"),
                query_col=step.get("query_col"),
                locale_col=step.get("locale_col"),
                slug_col=step.get("slug_col"),
            )
        elif t == "sample_paths":
            stream = stream.sample_paths(
                n=step.get("n"),
                frac=step.get("frac"),
                random_state=step.get("random_state"),
            )
        elif t == "split_sessions":
            stream = stream.split_sessions(
                session_id_col=step.get("session_id_col", "session_id"),
                session_index_col=step.get("session_index_col", "session_index"),
                separator=step.get("separator"),
                start_event=step.get("start_event"),
                end_event=step.get("end_event"),
                timeout=step.get("timeout"),
                path_col=step.get("path_col"),
            )
        else:
            raise ValueError(
                f"Unknown preprocessor type: {t!r}. "
                "Supported: collapse_events, filter_paths, filter_events, "
                "truncate_paths, rename_events, edit_events, drop_events, "
                "add_events, to_daily_states, add_segment, drop_segment, "
                "add_clusters, urls_to_events, sample_paths, split_sessions."
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


def _step_to_code(step: dict) -> str:
    """Convert a preprocessor step dict to its Python Eventstream call."""
    t = step.get("type", "")
    args = {k: v for k, v in step.items() if k != "type"}
    kw = ", ".join(f"{k}={v!r}" for k, v in args.items())

    if t == "collapse_events":
        if args.get("consecutive"):
            return "stream.collapse_events(consecutive=True)"
        elif args.get("event_groups"):
            return f"stream.collapse_events(event_groups={args['event_groups']!r})"
    elif t == "filter_events":
        if args.get("keep"):
            return f"stream.filter_events(keep={args['keep']!r})"
        elif args.get("drop"):
            return f"stream.filter_events(drop={args['drop']!r})"
        elif args.get("sql"):
            return f"stream.filter_events(sql={args['sql']!r})"
    elif t == "filter_paths":
        return f"stream.filter_paths({args!r})"
    elif t == "add_segment":
        if args.get("funnel_events"):
            return f"stream.add_segment(name={args['name']!r}, funnel_events={args['funnel_events']!r})"
        elif args.get("rules"):
            return f"stream.add_segment(name={args['name']!r}, rules={args['rules']!r})"
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
