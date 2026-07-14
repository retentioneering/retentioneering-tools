import json

import traitlets

from retentioneering.exceptions import RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._utils import parse_diff as _parse_diff
from retentioneering.widgets._html_export import write_html


class TransitionGraphWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("transition_graph").tag(sync=True)

    # ── recompute triggers ─────────────────────────────────────────────────────
    edge_weight = traitlets.Unicode("proba_out").tag(sync=True)
    diff = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ─────────────────────────────────────────────────────────────
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    event_counts = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g1 = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g2 = traitlets.Unicode("{}").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ─────
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── display / persistent ───────────────────────────────────────────────────
    height = traitlets.Int(500).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
    node_positions = traitlets.Unicode("{}").tag(sync=True)
    event_visibility = traitlets.Unicode("{}").tag(sync=True)
    # "" | "[min, max]" — edge weight filter, normalized to 0..1
    edge_filter = traitlets.Unicode("").tag(sync=True)
    # "" | "[min, max]" — event count (population) filter, absolute counts
    event_count_filter = traitlets.Unicode("").tag(sync=True)
    # "" | '{"zoom": z, "pan": {"x": x, "y": y}}' — canvas zoom/pan
    viewport = traitlets.Unicode("").tag(sync=True)

    _persist_names = (
        "edge_weight",
        "diff",
        "path_col",
        "height",
        "sidebar_open",
        "node_positions",
        "event_visibility",
        "edge_filter",
        "event_count_filter",
        "viewport",
    )

    def __init__(
        self,
        eventstream,
        edge_weight=_UNSET,
        diff=_UNSET,
        path_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self._load_state_file(state_file)

        try:
            self.segment_levels = json.dumps(eventstream.get_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)
        try:
            self.event_counts = json.dumps(eventstream.get_event_counts())
        except Exception:
            self.event_counts = "{}"

        self.edge_weight = edge_weight if edge_weight is not _UNSET else "proba_out"
        _diff_val = diff if diff is not _UNSET else None
        self.diff = json.dumps(list(_diff_val)) if _diff_val else ""
        self.path_col = path_col if path_col is not _UNSET else ""
        self.height = height if height is not _UNSET else 500
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True
        self.node_positions = "{}"

        self._apply_saved_state(
            exclude={
                name
                for name, arg in (
                    ("edge_weight", edge_weight),
                    ("diff", diff),
                    ("path_col", path_col),
                    ("height", height),
                    ("sidebar_open", sidebar_open),
                )
                if arg is not _UNSET
            }
        )

        self._recompute()

        self._initialized = True
        self.observe(self._on_params_change, names=["edge_weight", "diff", "path_col"])
        self.observe(self._on_positions_change, names=["node_positions"])
        self.observe(self._on_event_visibility_change, names=["event_visibility"])
        self._start_state_autosave()

    # ── widget-specific observers ──────────────────────────────────────────────

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    def _on_positions_change(self, _change):
        if not self._initialized:
            return

    def _on_event_visibility_change(self, _change):
        if not self._initialized:
            return

    # ── dispatch ───────────────────────────────────────────────────────────────

    def _tool_transition_graph_data(self, params: dict):
        return self._compute_tm_raw(
            edge_weight=params.get("edge_weight", self.edge_weight),
            path_col=params.get("path_col") or self.path_col or None,
            diff=_parse_diff(params.get("diff")),
        )

    def _tool_graph_layout(self, params: dict):
        return self._compute_graph_layout(params)

    #: See RetentioneeringWidget.compute_tools — a platform backend can call
    #: these directly via ``dispatch_compute`` (or by looking this table up
    #: on the class) instead of hand-rolling a tool -> Eventstream-method map.
    compute_tools = {
        "transition_graph_data": _tool_transition_graph_data,
        "graph_layout": _tool_graph_layout,
    }

    # ── computations ───────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            diff_list = _parse_diff(self.diff)
            result = self._compute_tm_raw(
                edge_weight=self.edge_weight,
                path_col=self.path_col or None,
                diff=diff_list,
            )
            self.result = json.dumps(result)

            if diff_list:
                try:
                    s1, s2 = self._eventstream._split_two(
                        diff_list, path_col=self.path_col or None
                    )
                    c1 = s1.get_event_counts()
                    c2 = s2.get_event_counts()
                    pid = self.path_col or s1.schema.path_cols[0]
                    for counts, s in ((c1, s1), (c2, s2)):
                        n = int(s.df[pid].nunique())
                        counts.setdefault("path_start", n)
                        counts.setdefault("path_end", n)
                    self.event_counts_g1 = json.dumps(c1)
                    self.event_counts_g2 = json.dumps(c2)
                except Exception:
                    self.event_counts_g1 = "{}"
                    self.event_counts_g2 = "{}"
            else:
                self.event_counts_g1 = "{}"
                self.event_counts_g2 = "{}"
        except RetentioneeringError:
            raise
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_tm_raw(self, edge_weight: str, path_col=None, diff=None) -> dict:
        tm = self._eventstream.transition_graph_data(
            edge_weight=edge_weight,
            path_col=path_col,
            diff=diff,
        )
        if diff is not None:
            tm, tm1, tm2 = tm
            return {
                "events": tm.index.tolist(),
                "values": _df_to_list(tm),
                "group1": {"events": tm1.index.tolist(), "values": _df_to_list(tm1)},
                "group2": {"events": tm2.index.tolist(), "values": _df_to_list(tm2)},
            }
        return {"events": tm.index.tolist(), "values": _df_to_list(tm)}

    def _compute_graph_layout(self, params: dict) -> dict:
        try:
            from retentioneering.tools.graph_layout import GraphLayout  # type: ignore

            result = GraphLayout(self._eventstream).fit(
                sample_size=params.get("sample_size", 1000),
                embedding_dim=params.get("embedding_dim", 32),
                n_clusters=params.get("n_clusters", 5),
                random_state=params.get("random_state", 42),
            )
            return {"result": result}
        except Exception:
            return {"result": {}}

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Transition Graph",
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        """
        Export the current graph as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab.
        analysis:
            Optional analysis text. Wrap event names in square brackets to make
            them clickable, e.g. `"Drop-off at [basket]: 78% of users leave here."`.
            Supports basic markdown (bold, italic, bullet lists, tables, headings).
        sidebar_open:
            Whether the settings sidebar starts open in the exported file.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        self._raise_if_error()
        data = {
            "widget_type": "transition_graph",
            "result": json.loads(self.result or "{}"),
            "edge_weight": self.edge_weight,
            "diff": json.loads(self.diff) if self.diff else None,
            "event_counts": json.loads(self.event_counts or "{}"),
            "event_counts_g1": json.loads(self.event_counts_g1 or "{}"),
            "event_counts_g2": json.loads(self.event_counts_g2 or "{}"),
            "node_positions": json.loads(self.node_positions or "{}"),
            "event_visibility": json.loads(self.event_visibility or "{}"),
            "edge_filter": json.loads(self.edge_filter) if self.edge_filter else None,
            "event_count_filter": json.loads(self.event_count_filter)
            if self.event_count_filter
            else None,
            "viewport": json.loads(self.viewport) if self.viewport else None,
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "path_cols": json.loads(self.path_cols or "[]"),
            "path_col": self.path_col or "",
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
        }
        write_html(path, title, "Transition Graph", data, analysis)


# ── helpers ────────────────────────────────────────────────────────────────────


def _df_to_list(df) -> list:
    import pandas as pd

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
