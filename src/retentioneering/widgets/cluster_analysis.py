import json
from dataclasses import asdict
from typing import TYPE_CHECKING

import traitlets

if TYPE_CHECKING:
    import pandas as pd

from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._html_export import write_html


class ClusterAnalysisWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("cluster_analysis").tag(sync=True)

    # ── config ─────────────────────────────────────────────────────────────
    features = traitlets.Unicode("[]").tag(sync=True)  # JSON list of metric configs
    method = traitlets.Unicode("kmeans").tag(sync=True)
    scaler = traitlets.Unicode("minmax").tag(sync=True)
    n_clusters = traitlets.Unicode("").tag(sync=True)  # "" | "3" | "3-8" | "[3,4,5]"
    nmf_components = traitlets.Unicode("").tag(sync=True)  # "" | "3" | "3,5,7"
    nmf_enabled = traitlets.Bool(False).tag(sync=True)
    overview_metrics = traitlets.Unicode("[]").tag(sync=True)
    aggregation = traitlets.Unicode("mean").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)
    apply_trigger = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ─────────────────────────────────────────────────────────
    event_list = traitlets.Unicode("[]").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ─
    result = traitlets.Unicode("{}").tag(sync=True)
    # Concrete params (e.g. the n_clusters that won the silhouette grid search)
    # that produced `result` — pass straight to add_clusters to reproduce it.
    chosen_params = traitlets.Unicode("{}").tag(sync=True)

    # ── display ────────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(520).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
    # "" | tab name ("Overview", "Silhouette", ...) — active result tab
    active_tab = traitlets.Unicode("").tag(sync=True)
    # '{}' | '{"cluster_0": "power users", ...}' — display renames typed into
    # the heatmap header (cleared on re-clustering)
    cluster_renames = traitlets.Unicode("{}").tag(sync=True)
    # Name of the caller's eventstream variable (best-effort) - used by the JS
    # side to render copy-pasteable code that refers to it by its real name.
    stream_var_name = traitlets.Unicode("stream").tag(sync=True)

    # ── save clusters to eventstream ─────────────────────────────────────────
    save_segment_name = traitlets.Unicode("").tag(sync=True)
    save_rename = traitlets.Unicode("{}").tag(sync=True)  # JSON {old_label: new_label}
    save_trigger = traitlets.Unicode("").tag(sync=True)
    save_result = traitlets.Unicode("{}").tag(
        sync=True
    )  # JSON {ok, segment_name, error}

    # ── distribution request/result ───────────────────────────────────────
    dist_request = traitlets.Unicode("").tag(sync=True)
    dist_result = traitlets.Unicode("{}").tag(sync=True)

    _persist_names = (
        "features",
        "method",
        "scaler",
        "n_clusters",
        "nmf_components",
        "nmf_enabled",
        "overview_metrics",
        "aggregation",
        "path_col",
        "height",
        "sidebar_open",
        "active_tab",
        "cluster_renames",
    )

    def __init__(
        self,
        eventstream,
        features=_UNSET,
        method=_UNSET,
        scaler=_UNSET,
        n_clusters=_UNSET,
        overview_metrics=_UNSET,
        path_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        stream_var_name=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self.widget_id = ""
        self.widget_type = "cluster_analysis"
        self._load_state_file(state_file)
        # Cluster labels (path id -> "cluster_0"/.../"noise") from the last
        # successful Apply - cached so a distribution request can rebuild the
        # exact clustering shown in the heatmap without re-running it.
        self._cluster_labels: "pd.Series | None" = None
        self.stream_var_name = (
            stream_var_name if stream_var_name not in (_UNSET, None) else "stream"
        )

        # Catalogues
        try:
            all_events = sorted(
                eventstream.df[eventstream.schema.event_col]
                .astype(str)
                .unique()
                .tolist()
            )
            self.event_list = json.dumps(all_events)
        except Exception:
            self.event_list = "[]"
        self.path_cols = json.dumps(eventstream.schema.path_cols)
        self.segment_cols = json.dumps(eventstream.schema.segment_cols)
        try:
            self.segment_levels = json.dumps(eventstream.get_segment_values())
        except Exception:
            self.segment_levels = "{}"

        _feat = features if features is not _UNSET else None
        if _feat is None:
            # No 'events' -> wildcard (all events), same as leaving it empty in the UI.
            _feat = [{"metric": "event_count"}]
        self.features = (
            json.dumps(_feat) if isinstance(_feat, list) else (_feat or "[]")
        )
        self.method = method if method is not _UNSET else "kmeans"
        self.scaler = scaler if scaler is not _UNSET else "minmax"
        _nc = n_clusters if n_clusters is not _UNSET else ""
        self.n_clusters = (
            json.dumps(_nc) if isinstance(_nc, list) else (str(_nc) if _nc else "3-8")
        )
        self.nmf_enabled = False
        self.nmf_components = ""
        _mc = overview_metrics if overview_metrics is not _UNSET else None
        if _mc is None:
            _mc = [{"metric": "event_count", "agg": "mean"}]
        self.overview_metrics = (
            json.dumps(_mc) if isinstance(_mc, list) else (_mc or "[]")
        )
        self.aggregation = "mean"
        self.path_col = path_col if path_col is not _UNSET else ""
        self.height = height if height is not _UNSET else 520
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._apply_saved_state(
            exclude={
                name
                for name, arg in (
                    ("features", features),
                    ("method", method),
                    ("scaler", scaler),
                    ("n_clusters", n_clusters),
                    ("overview_metrics", overview_metrics),
                    ("path_col", path_col),
                    ("height", height),
                    ("sidebar_open", sidebar_open),
                )
                if arg is not _UNSET
            }
        )

        self._initialized = True
        self.observe(self._on_apply, names=["apply_trigger"])
        self.observe(self._on_save, names=["save_trigger"])
        self.observe(self._on_dist_request, names=["dist_request"])
        self._start_state_autosave()

        # Auto-compute when features were explicitly provided or restored
        # from a state file.
        if features is not _UNSET or self._saved_state:
            self._recompute()

    # ── observers ──────────────────────────────────────────────────────────

    def _on_apply(self, _change):
        if not self._initialized:
            return
        self._recompute()

    def _on_save(self, _change):
        if not self._initialized:
            return
        self._save_clusters()

    def _on_dist_request(self, change):
        raw = change["new"]
        if not raw:
            return
        try:
            req = json.loads(raw)
        except Exception:
            return
        self._compute_distribution(req)

    # ── dispatch ───────────────────────────────────────────────────────────

    def _tool_cluster_analysis_data(self, params: dict):
        features = params.get("features")
        if features is None:
            features = json.loads(self.features) if self.features else []
        overview_metrics = params.get("overview_metrics")
        if overview_metrics is None:
            overview_metrics = (
                json.loads(self.overview_metrics) if self.overview_metrics else []
            )
        n_clusters_raw = params.get("n_clusters", self.n_clusters)
        nmf_raw = params.get(
            "nmf_components", self.nmf_components if self.nmf_enabled else ""
        )
        return self._compute_raw(
            features=features,
            method=params.get("method", self.method),
            scaler=params.get("scaler", self.scaler) or None,
            n_clusters=_parse_n_clusters(_as_n_clusters_str(n_clusters_raw)),
            nmf_components=(
                _parse_n_clusters(_as_n_clusters_str(nmf_raw)) if nmf_raw else None
            ),
            overview_metrics=overview_metrics,
            aggregation=params.get("aggregation", self.aggregation or "mean"),
            path_col=params.get("path_col") or self.path_col or None,
        )["result"]

    def _tool_get_metric_distribution(self, params: dict):
        return self._compute_distribution_raw(
            metric=params["metric"],
            segment_value=params["segment_value"],
            complement=params.get("complement", False),
            path_col=params.get("path_col") or self.path_col or None,
        )

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {
        "cluster_analysis_data": _tool_cluster_analysis_data,
        "get_metric_distribution": _tool_get_metric_distribution,
    }

    # ── computation ────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            features = json.loads(self.features) if self.features else []
            if not features:
                self.result = "{}"
                self._cluster_labels = None
                return
            metrics = json.loads(self.overview_metrics) if self.overview_metrics else []
            n_clusters = _parse_n_clusters(self.n_clusters)
            nmf_components = (
                _parse_n_clusters(self.nmf_components)
                if self.nmf_enabled and self.nmf_components
                else None
            )

            computed = self._compute_raw(
                features=features,
                method=self.method,
                scaler=self.scaler or None,
                n_clusters=n_clusters,
                nmf_components=nmf_components,
                overview_metrics=metrics,
                aggregation=self.aggregation or "mean",
                path_col=self.path_col or None,
            )

            self.result = json.dumps(computed["result"])
            self.chosen_params = json.dumps(computed["best_params"])
            self._cluster_labels = computed["cluster_labels"]
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
            self.chosen_params = "{}"
            self._cluster_labels = None
        finally:
            self.is_loading = False

    def _compute_raw(
        self,
        features,
        method,
        scaler,
        n_clusters,
        nmf_components,
        overview_metrics,
        aggregation="mean",
        path_col=None,
    ) -> dict:
        # Apply global aggregation to metrics that don't have their own agg.
        metrics = [{**m, "agg": m.get("agg") or aggregation} for m in overview_metrics]

        raw = self._eventstream.cluster_analysis_data(
            features=features,
            method=method,
            scaler=scaler,
            n_clusters=n_clusters,
            nmf_components=nmf_components,
            overview_metrics=metrics,
            path_col=path_col,
        )

        result: dict = {}
        if "overview_df" in raw and raw["overview_df"] is not None:
            df = raw["overview_df"]
            result["overview"] = {
                "metrics": df.index.tolist(),
                "segments": df.columns.tolist(),
                "values": [[_safe(v) for v in df.loc[m].tolist()] for m in df.index],
            }
        if "silhouette" in raw:
            sil = raw["silhouette"]
            result["silhouette"] = {
                "params": sil["params"],
                "silhouette": [_safe(s) for s in sil["silhouette"]],
            }
        if "nmf" in raw and raw["nmf"] is not None:
            result["nmf"] = raw["nmf"]

        return {
            "result": result,
            "best_params": raw.get("best_params") or {},
            "cluster_labels": raw.get("cluster_labels"),
        }

    def _compute_distribution_raw(
        self, metric, segment_value, complement=False, path_col=None
    ) -> dict:
        from retentioneering.tools.cluster_analysis import ClusterAnalysis

        if self._cluster_labels is None:
            raise ValueError("No clustering result to compare - click Apply first.")
        return ClusterAnalysis(self._eventstream).get_metric_distribution(
            cluster_labels=self._cluster_labels,
            metric=metric,
            segment_value=segment_value,
            complement=complement,
            path_col=path_col,
        )

    def _compute_distribution(self, req: dict):
        self.is_loading = True
        try:
            result = self._compute_distribution_raw(
                metric=req["metric"],
                segment_value=req["segment_value"],
                complement=req.get("complement", False),
                path_col=self.path_col or None,
            )
            self.dist_result = json.dumps(
                result,
                default=lambda x: None if (isinstance(x, float) and x != x) else x,
            )
        except Exception as exc:
            self.dist_result = json.dumps({"error": str(exc)})
        finally:
            self.is_loading = False

    def _save_clusters(self):
        """Materialize the clustering shown in `result` as a segment column.

        Mutates the shared `self._eventstream` object (the same object the
        caller's variable points to) so the change is visible without
        reassignment. This is a deliberate exception to the rest of the
        codebase's immutable-Eventstream convention (ADR-0003) and is
        irreversible from the widget - if the user doesn't like the result
        they must re-run the cell that built the eventstream from scratch.
        Other widgets already open on the same eventstream won't see the new
        column until re-created since their catalogs are snapshotted at
        construction time.

        The JS side offers a "Copy code" alternative that renders the
        equivalent `add_clusters(...)` call without touching the eventstream
        at all — that one is computed entirely client-side (see
        `chosen_params` below) since it's pure templating, so it doesn't need
        a round trip through this method.
        """
        try:
            name = (self.save_segment_name or "").strip()
            if not name:
                raise ValueError("Segment column name is required.")

            features = json.loads(self.features) if self.features else []
            if not features:
                raise ValueError("No features configured - nothing to cluster.")

            rename = json.loads(self.save_rename) if self.save_rename else {}
            params = json.loads(self.chosen_params) if self.chosen_params else {}

            kwargs: dict = {
                "name": name,
                "features": features,
                "method": self.method,
                "scaler": self.scaler or None,
                "path_col": self.path_col or None,
            }
            if self.method == "kmeans":
                kwargs["n_clusters"] = params.get("n_clusters")
            else:
                kwargs["min_cluster_size"] = params.get("min_cluster_size")
                kwargs["cluster_selection_epsilon"] = params.get(
                    "cluster_selection_epsilon"
                )
            if params.get("nmf_components") is not None:
                kwargs["nmf_components"] = params["nmf_components"]

            self._apply_clusters_inplace(kwargs, rename)
            self.save_result = json.dumps({"ok": True, "segment_name": name})
        except Exception as exc:
            self.save_result = json.dumps({"ok": False, "error": str(exc)})

    def _apply_clusters_inplace(self, kwargs: dict, rename: dict) -> None:
        from retentioneering.data_processors.add_clusters import AddClusters
        from retentioneering.data_processors.rename_segment_values import (
            RenameSegmentValues,
        )

        new_df, new_schema = AddClusters(eventstream=self._eventstream, **kwargs).apply(
            self._eventstream.df, self._eventstream.schema
        )
        if rename:
            new_df, new_schema = RenameSegmentValues(kwargs["name"], rename).apply(
                new_df, new_schema
            )

        es = self._eventstream
        es._df = new_df
        es._schema = asdict(new_schema)
        # `schema`/`fingerprint` are cached_property - drop the stale cached values
        # so the next access recomputes them from the new _df/_schema.
        es.__dict__.pop("schema", None)
        es.__dict__.pop("fingerprint", None)

        # Refresh this widget's own catalogs so its sidebar reflects the new column.
        self.segment_cols = json.dumps(es.schema.segment_cols)
        try:
            self.segment_levels = json.dumps(es.get_segment_values())
        except Exception:
            pass

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Cluster Analysis",
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        """
        Export the cluster analysis as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab.
        analysis:
            Optional analysis text. Supports basic markdown and [event] links.
        sidebar_open:
            Whether the settings sidebar starts open in the exported file.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        data = {
            "widget_type": "cluster_analysis",
            "result": json.loads(self.result or "{}"),
            "features": json.loads(self.features or "[]"),
            "method": self.method,
            "scaler": self.scaler,
            "n_clusters": self.n_clusters,
            "nmf_components": self.nmf_components,
            "nmf_enabled": self.nmf_enabled,
            "overview_metrics": json.loads(self.overview_metrics or "[]"),
            "aggregation": self.aggregation,
            "path_col": self.path_col or "",
            "path_cols": json.loads(self.path_cols or "[]"),
            "segment_cols": json.loads(self.segment_cols or "[]"),
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "event_list": json.loads(self.event_list or "[]"),
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
            "active_tab": self.active_tab or "",
            "cluster_renames": json.loads(self.cluster_renames or "{}"),
        }
        write_html(path, title, "Cluster Analysis", data, analysis)


# ── helpers ───────────────────────────────────────────────────────────────────


def _safe(v):
    import math

    if v is None:
        return None
    try:
        # Convert numpy scalars to Python native types
        if hasattr(v, "item"):
            v = v.item()
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
    except Exception:
        return None
    return v


def _as_n_clusters_str(value) -> str:
    """Normalize a compute_request n_clusters/nmf_components param (which may
    arrive as an int, a list, or already a string) into the string format
    `_parse_n_clusters` expects — the same normalization the constructor
    already applies to the `n_clusters`/`nmf_components` traitlets.
    """
    if isinstance(value, list):
        return json.dumps(value)
    return str(value) if value else ""


def _parse_n_clusters(raw: str):
    if not raw or raw.strip() == "":
        return None
    s = raw.strip()
    try:
        # Range notation: "3-8" → [3,4,5,6,7,8]
        if "-" in s and not s.startswith("[") and not s.startswith("-"):
            parts = s.split("-")
            if len(parts) == 2:
                lo, hi = int(parts[0].strip()), int(parts[1].strip())
                return list(range(lo, hi + 1))
        # JSON list: "[3,4,5]"
        if s.startswith("["):
            return json.loads(s)
        # Comma-separated: "3,4,5"
        if "," in s:
            return [int(x.strip()) for x in s.split(",")]
        return int(s)
    except Exception:
        return None
