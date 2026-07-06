import json
import pathlib

import anywidget
import traitlets

from retentioneering.widgets._esm import _get_esm
from retentioneering.widgets.cloud_mixin import CloudMixin
from retentioneering.widgets._utils import parse_diff as _parse_diff
from retentioneering.widgets._html_export import write_html

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()


class TransitionGraphWidget(CloudMixin, anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

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

    # ── result ─────────────────────────────────────────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    # ── display / persistent ───────────────────────────────────────────────────
    height = traitlets.Int(500).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
    node_positions = traitlets.Unicode("{}").tag(sync=True)
    event_visibility = traitlets.Unicode("{}").tag(sync=True)

    # ── generic compute protocol ───────────────────────────────────────────────
    compute_request = traitlets.Unicode("").tag(sync=True)
    compute_response = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        eventstream,
        cloud_file_name: str | None = None,
        edge_weight=_UNSET,
        diff=_UNSET,
        path_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False

        try:
            self.segment_levels = json.dumps(eventstream.get_segment_values())
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

        self._init_cloud(cloud_file_name)

        if not self._cloud_file_name:
            self._recompute()

        self._initialized = True
        self.observe(self._on_params_change, names=["edge_weight", "diff", "path_col"])
        self.observe(self._on_positions_change, names=["node_positions"])
        self.observe(self._on_event_visibility_change, names=["event_visibility"])
        self.observe(self._on_compute_request, names=["compute_request"])

    # ── widget-specific observers ──────────────────────────────────────────────

    def _on_params_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        self._recompute()
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_positions_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_event_visibility_change(self, _change):
        if not self._initialized or self._loading_from_cloud:
            return
        if self._guard_auto_save():
            self._schedule_cloud_save()

    def _on_compute_request(self, change):
        raw = change["new"]
        if not raw:
            return
        try:
            req = json.loads(raw)
        except Exception:
            return
        req_id = req.get("id", "")
        tool = req.get("tool", "")
        params = req.get("params", {})
        try:
            result = self._dispatch(tool, params)
            self.compute_response = json.dumps({"id": req_id, "result": result})
        except Exception as exc:
            self.compute_response = json.dumps({"id": req_id, "error": str(exc)})

    # ── cloud state ────────────────────────────────────────────────────────────

    def _current_state(self) -> dict:
        return {
            **self._base_state(),
            "params": {
                "edge_weight": self.edge_weight,
                "diff": self.diff,
                "path_col": self.path_col,
            },
            "display": {
                "height": self.height,
                "sidebar_open": self.sidebar_open,
            },
            "node_positions": json.loads(self.node_positions or "{}"),
            "event_visibility": json.loads(self.event_visibility or "{}"),
        }

    def _apply_state(self, state: dict) -> None:
        p = state.get("params", {})
        d = state.get("display", {})

        self.edge_weight = p.get("edge_weight", "proba_out")
        self.height = d.get("height", 500)
        self.sidebar_open = d.get("sidebar_open", True)
        pos = state.get("node_positions", {})
        self.node_positions = json.dumps(pos) if pos else "{}"
        ev = state.get("event_visibility", {})
        self.event_visibility = json.dumps(ev) if ev else "{}"

        _diff, _pid, _mismatch = self._apply_base_state(state)
        self.diff = json.dumps(list(_diff)) if _diff else ""
        self.path_col = _pid

        self._recompute()

    # ── dispatch ───────────────────────────────────────────────────────────────

    def _dispatch(self, tool: str, params: dict):
        if tool == "transition_graph_data":
            return self._compute_tm_raw(
                edge_weight=params.get("edge_weight", self.edge_weight),
                path_col=params.get("path_col") or self.path_col or None,
                diff=_parse_diff(params.get("diff")),
            )
        if tool == "graph_layout":
            return self._compute_graph_layout(params)
        raise ValueError(f"Unknown tool: {tool!r}")

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
        sidebar_open: bool = True,
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
        """
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
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "path_cols": json.loads(self.path_cols or "[]"),
            "path_col": self.path_col or "",
            "height": self.height,
            "sidebar_open": sidebar_open,
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
