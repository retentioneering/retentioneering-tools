import json
import pathlib
from dataclasses import asdict

import anywidget
import traitlets

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()

from retentioneering.widgets._esm import _get_esm  # noqa: E402
from retentioneering.widgets._html_export import write_html  # noqa: E402


class ClusterAnalysisWidget(anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

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

    # ── result ─────────────────────────────────────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)
    # Concrete params (e.g. the n_clusters that won the silhouette grid search)
    # that produced `result` — pass straight to add_clusters to reproduce it.
    chosen_params = traitlets.Unicode("{}").tag(sync=True)

    # ── display ────────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(520).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self.widget_id = ""
        self.widget_type = "cluster_analysis"
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

        self._initialized = True
        self.observe(self._on_apply, names=["apply_trigger"])
        self.observe(self._on_save, names=["save_trigger"])

        # Auto-compute when features were explicitly provided
        if features is not _UNSET:
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

    # ── computation ────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            features = json.loads(self.features) if self.features else []
            if not features:
                self.result = "{}"
                return
            metrics = json.loads(self.overview_metrics) if self.overview_metrics else []
            agg = self.aggregation or "mean"
            # Apply global aggregation to metrics that don't have their own agg
            metrics = [{**m, "agg": m.get("agg") or agg} for m in metrics]
            n_clusters = _parse_n_clusters(self.n_clusters)
            nmf_components = (
                _parse_n_clusters(self.nmf_components)
                if self.nmf_enabled and self.nmf_components
                else None
            )
            pid = self.path_col or None

            raw = self._eventstream.cluster_analysis_data(
                features=features,
                method=self.method,
                scaler=self.scaler or None,
                n_clusters=n_clusters,
                nmf_components=nmf_components,
                overview_metrics=metrics,
                path_col=pid,
            )

            result: dict = {}
            if "overview_df" in raw and raw["overview_df"] is not None:
                df = raw["overview_df"]
                result["overview"] = {
                    "metrics": df.index.tolist(),
                    "segments": df.columns.tolist(),
                    "values": [
                        [_safe(v) for v in df.loc[m].tolist()] for m in df.index
                    ],
                }
            if "silhouette" in raw:
                sil = raw["silhouette"]
                result["silhouette"] = {
                    "params": sil["params"],
                    "silhouette": [_safe(s) for s in sil["silhouette"]],
                }
            if "nmf" in raw and raw["nmf"] is not None:
                result["nmf"] = raw["nmf"]

            self.result = json.dumps(result)
            self.chosen_params = json.dumps(raw.get("best_params") or {})
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
            self.chosen_params = "{}"
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
