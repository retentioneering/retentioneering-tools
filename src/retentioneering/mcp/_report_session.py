"""`ReportSession`: the MCP agent's per-`serve()`-call state — the active
(possibly preprocessed) stream, which events the caller flagged as important,
the base preprocessors applied so far, and the pending report tabs. Replaces
what used to be four separate closure variables rebound by hand inside
`_build_server`'s nested tool functions; `server.py`'s tool wrappers now just
call methods on one `ReportSession` instance instead of mutating list-boxed
free variables directly — the same "business logic behind a small object,
transport wires it up" split `widgets/_base.py`'s `RetentioneeringWidget`
(`dispatch_compute`) already uses for the REST/anywidget transport boundary.
"""

from __future__ import annotations

import pathlib
import tempfile
from typing import Any

from retentioneering.mcp._agent_logic import _apply_preprocessors, _build_data_note


class ReportSession:
    def __init__(self, base_stream: Any, context: dict):
        self._base_stream = base_stream  # original, never mutated
        self.active_stream = base_stream
        self.context_events: set = set(context.get("events", {}).keys())
        self.base_preprocessors: list = []
        self.pending_tabs: list[dict] = []

    @staticmethod
    def stream_stats(stream: Any) -> dict:
        s = stream.schema
        df = stream.df
        return {
            "n_paths": int(df[s.path_col].nunique()),
            "n_events_total": len(df),
            "events": sorted(df[s.event_col].astype(str).unique().tolist()),
        }

    def update_base_stream(self, preprocessors: list) -> Any:
        """Always applies to the ORIGINAL stream, not `active_stream`, so
        repeated calls never stack filters accidentally."""
        new_stream = _apply_preprocessors(self._base_stream, preprocessors)
        self.active_stream = new_stream
        self.base_preprocessors = list(preprocessors)
        return new_stream

    def reset_base_stream(self) -> Any:
        self.active_stream = self._base_stream
        self.base_preprocessors = []
        return self._base_stream

    def add_tab(self, label: str, data: dict, local_preprocessors: list) -> str:
        tab_id = f"tab-{len(self.pending_tabs)}"
        self.pending_tabs.append(
            {"label": label, "data": data, "local_preprocessors": local_preprocessors}
        )
        return tab_id

    def export(self, title: str, analysis: str | None, path: str | None) -> dict:
        from retentioneering.widgets._html_export import write_report_html

        if path is None:
            tmp = tempfile.NamedTemporaryFile(
                suffix=".html", delete=False, prefix="retentioneering_"
            )
            path = tmp.name
            tmp.close()

        widgets = list(self.pending_tabs)
        data_sources_html = _build_data_note(list(self.base_preprocessors), widgets)
        write_report_html(
            path, title, widgets, analysis, data_sources_html=data_sources_html
        )
        self.pending_tabs.clear()  # only clear after successful write
        return {
            "path": str(pathlib.Path(path).resolve()),
            "title": title,
            "tabs": [w["label"] for w in widgets],
        }
