"""A/B test widget: the `get_ab_test` scorecard as a table, a forest plot and
strata / period panels.

Every number comes from `Eventstream.ab_test_data` (the widget's headless twin,
an alias of `get_ab_test`, ADR-0006/ADR-0017): the Scorecard is one call without
`strata_col` and `period`, the Strata tab one call with `strata_col`, the Periods
tab one call with `period`. The rows are sent as they come back, one dict per
row, with NaN / NA as null and timestamps as ISO strings.
"""

from __future__ import annotations

import json
import math
import warnings
from typing import Any

import numpy as np
import pandas as pd
import traitlets

from retentioneering.exceptions import RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._utils import (
    custom_columns_catalogue as _custom_columns_catalogue,
)
from retentioneering.widgets._utils import parse_diff as _parse_diff

#: 0/1 per path: shown as percentages, deltas in percentage points.
_BINARY_METRICS = frozenset(
    {"has_event", "has_any_event", "has_all_events", "matches_pattern", "in_segment"}
)
#: Per-path totals (a silent path counts 0).
_COUNT_METRICS = frozenset({"event_count", "length", "active_days", "column_sum"})
#: Metrics measured in seconds.
_SECONDS_METRICS = frozenset({"duration", "time_between"})

_DEFAULTS: dict[str, Any] = {
    "strata_col": "",
    "period": "",
    "period_basis": "calendar",
    "period_population": "itt",
    "horizon": "",
    "test": "auto",
    "correction": "holm",
    "correction_scope": "metric",
    "ci_level": 0.95,
    "power": 0.8,
    "n_bootstrap": "1000",
    "bootstrap_weights": "multinomial",
    "min_support": 10,
    "random_state": 42,
    "path_col": "",
    "strata_pre_exposure": None,
}


def _duration_str(value) -> str:
    """A `period` / `horizon` argument as the string the traitlet holds."""
    if value is None:
        return ""
    if isinstance(value, pd.Timedelta):
        seconds = value.total_seconds()
        if seconds % 86400 == 0:
            return f"{int(seconds // 86400)}D"
        return f"{seconds:g}s"
    if isinstance(value, pd.DateOffset):
        return value.freqstr
    return str(value)


def _metric_kind(entry: dict) -> str:
    """`binary`, `count`, `value` or `ratio`, as `get_ab_test` reads the entry."""
    if entry.get("denominator") is not None:
        return "ratio"
    metric = entry.get("metric")
    if metric in _BINARY_METRICS:
        return "binary"
    if metric in _COUNT_METRICS:
        return "count"
    return "value"


def _cell(value: Any) -> Any:
    """One scorecard cell as strict JSON: null for NaN / NA, ISO for times."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        value = float(value)
        return value if math.isfinite(value) else None
    return value


def scorecard_records(df: pd.DataFrame | None) -> list[dict] | None:
    """The scorecard frame as one dict per row, in its column order."""
    if df is None:
        return None
    columns = list(df.columns)
    return [
        {column: _cell(value) for column, value in zip(columns, row)}
        for row in df.itertuples(index=False, name=None)
    ]


class ABTestWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("ab_test").tag(sync=True)

    # ── recompute triggers (the data parameters of ab_test_data) ─────────────
    diff = traitlets.Unicode("").tag(sync=True)
    metrics = traitlets.Unicode("[]").tag(sync=True)
    strata_col = traitlets.Unicode("").tag(sync=True)
    strata_pre_exposure = traitlets.Bool(None, allow_none=True).tag(sync=True)
    period = traitlets.Unicode("").tag(sync=True)
    period_basis = traitlets.Unicode("calendar").tag(sync=True)
    period_population = traitlets.Unicode("itt").tag(sync=True)
    horizon = traitlets.Unicode("").tag(sync=True)
    test = traitlets.Unicode("auto").tag(sync=True)
    correction = traitlets.Unicode("holm").tag(sync=True)
    correction_scope = traitlets.Unicode("metric").tag(sync=True)
    ci_level = traitlets.Float(0.95).tag(sync=True)
    power = traitlets.Float(0.8).tag(sync=True)
    # an int or "auto"; kept as text so the JS side reads one type
    n_bootstrap = traitlets.Unicode("1000").tag(sync=True)
    bootstrap_weights = traitlets.Unicode("multinomial").tag(sync=True)
    min_support = traitlets.Int(10).tag(sync=True)
    random_state = traitlets.Int(42).tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ───────────────────────────────────────────────────────────
    event_list = traitlets.Unicode("[]").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)
    custom_cols = traitlets.Unicode("{}").tag(sync=True)
    #: "" without an assignment table, "arm" with one, "arm,assigned_at" when it
    #: has assignment times (horizon and since-assignment periods need them).
    assignment_info = traitlets.Unicode("").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ────
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── display ──────────────────────────────────────────────────────────────
    height = traitlets.Int(560).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    _data_names = (
        "diff",
        "metrics",
        "strata_col",
        "strata_pre_exposure",
        "period",
        "period_basis",
        "period_population",
        "horizon",
        "test",
        "correction",
        "correction_scope",
        "ci_level",
        "power",
        "n_bootstrap",
        "bootstrap_weights",
        "min_support",
        "random_state",
        "path_col",
    )
    _persist_names = _data_names + ("height", "sidebar_open")

    def __init__(
        self,
        eventstream,
        diff=_UNSET,
        metrics=_UNSET,
        assignment=None,
        strata_col=_UNSET,
        period=_UNSET,
        period_basis=_UNSET,
        period_population=_UNSET,
        horizon=_UNSET,
        test=_UNSET,
        correction=_UNSET,
        correction_scope=_UNSET,
        ci_level=_UNSET,
        power=_UNSET,
        n_bootstrap=_UNSET,
        bootstrap_weights=_UNSET,
        min_support=_UNSET,
        random_state=_UNSET,
        path_col=_UNSET,
        strata_pre_exposure=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        # A table, not JSON: kept on the instance, never synced or saved.
        self._assignment = assignment
        self._initialized = False
        self._load_state_file(state_file)

        schema = eventstream.schema
        try:
            self.event_list = json.dumps(
                sorted(eventstream.df[schema.event_col].astype(str).unique().tolist())
            )
        except Exception:
            self.event_list = "[]"
        try:
            self.segment_levels = json.dumps(eventstream.get_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.segment_cols = json.dumps(list(schema.segment_cols or []))
        self.path_cols = json.dumps(list(schema.path_cols))
        self.custom_cols = _custom_columns_catalogue(eventstream)
        self.assignment_info = _assignment_info(assignment)

        args = {
            "diff": diff,
            "metrics": metrics,
            "strata_col": strata_col,
            "strata_pre_exposure": strata_pre_exposure,
            "period": period,
            "period_basis": period_basis,
            "period_population": period_population,
            "horizon": horizon,
            "test": test,
            "correction": correction,
            "correction_scope": correction_scope,
            "ci_level": ci_level,
            "power": power,
            "n_bootstrap": n_bootstrap,
            "bootstrap_weights": bootstrap_weights,
            "min_support": min_support,
            "random_state": random_state,
            "path_col": path_col,
            "height": height,
            "sidebar_open": sidebar_open,
        }
        self.diff = json.dumps(list(diff)) if diff is not _UNSET and diff else ""
        self.metrics = json.dumps(list(metrics) if metrics is not _UNSET else [])
        for name, default in _DEFAULTS.items():
            value = args[name]
            if value is _UNSET or (value is None and name != "strata_pre_exposure"):
                value = default
            elif name in ("period", "horizon"):
                value = _duration_str(value)
            elif name == "n_bootstrap":
                value = str(value)
            setattr(self, name, value)
        self.height = height if height is not _UNSET else 560
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._apply_saved_state(
            exclude={name for name, arg in args.items() if arg is not _UNSET}
        )

        self._recompute()
        self._initialized = True
        self.observe(self._on_params_change, names=list(self._data_names))
        self._start_state_autosave()

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    # ── parameters ───────────────────────────────────────────────────────────

    def _params(self, overrides: dict | None = None) -> dict:
        """The data parameters as `ab_test_data` keyword arguments."""
        raw = {name: getattr(self, name) for name in self._data_names}
        raw.update(overrides or {})
        n_bootstrap = raw["n_bootstrap"]
        if isinstance(n_bootstrap, str) and n_bootstrap.strip().lstrip("-").isdigit():
            n_bootstrap = int(n_bootstrap)
        metrics = raw["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics) if metrics else []
        return {
            "diff": _parse_diff(raw["diff"]),
            "metrics": metrics,
            "strata_col": raw["strata_col"] or None,
            "strata_pre_exposure": raw["strata_pre_exposure"],
            "period": raw["period"] or None,
            "period_basis": raw["period_basis"] or "calendar",
            "period_population": raw["period_population"] or "itt",
            "horizon": raw["horizon"] or None,
            "test": raw["test"] or "auto",
            "correction": raw["correction"] or "holm",
            "correction_scope": raw["correction_scope"] or "metric",
            "ci_level": float(raw["ci_level"]),
            "power": float(raw["power"]),
            "n_bootstrap": n_bootstrap,
            "bootstrap_weights": raw["bootstrap_weights"] or "multinomial",
            "min_support": int(raw["min_support"]),
            "random_state": int(raw["random_state"]),
            "path_col": raw["path_col"] or None,
        }

    # ── dispatch ─────────────────────────────────────────────────────────────

    def _tool_ab_test_data(self, params: dict):
        return self._compute_raw(self._params(params))

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {"ab_test_data": _tool_ab_test_data}

    # ── computation ──────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            self.result = self._encode_result(self._compute_raw(self._params()))
        except RetentioneeringError as exc:
            # At construction the caller's arguments are wrong: raise in the cell.
            # Later the change came from the sidebar: show it in the widget.
            if not self._initialized:
                raise
            self.error = str(exc)
            self.result = "{}"
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_raw(self, params: dict) -> dict:
        """The three scorecards of the widget, each straight from `ab_test_data`."""
        diff = params["diff"]
        metrics = params["metrics"]
        if not diff or not metrics:
            return {}
        common = {
            key: params[key]
            for key in (
                "horizon",
                "test",
                "correction",
                "correction_scope",
                "ci_level",
                "power",
                "n_bootstrap",
                "bootstrap_weights",
                "min_support",
                "random_state",
                "path_col",
            )
        }
        stream = self._eventstream
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            overall = stream.ab_test_data(
                diff, metrics, assignment=self._assignment, **common
            )
            strata = (
                stream.ab_test_data(
                    diff,
                    metrics,
                    assignment=self._assignment,
                    strata_col=params["strata_col"],
                    strata_pre_exposure=params["strata_pre_exposure"],
                    **common,
                )
                if params["strata_col"]
                else None
            )
            periods = (
                stream.ab_test_data(
                    diff,
                    metrics,
                    assignment=self._assignment,
                    period=params["period"],
                    period_basis=params["period_basis"],
                    period_population=params["period_population"],
                    **common,
                )
                if params["period"]
                else None
            )
        names = list(dict.fromkeys(overall["metric"].tolist()))
        kinds = [_metric_kind(entry) for entry in metrics]
        units = [
            "seconds"
            if entry.get("denominator") is None
            and entry.get("metric") in _SECONDS_METRICS
            else ""
            for entry in metrics
        ]
        if len(diff) == 3:
            labels = (str(diff[1]), str(diff[2]))
        else:
            labels = ("Group 1", "Group 2")
        return {
            "overall": scorecard_records(overall),
            "strata": scorecard_records(strata),
            "periods": scorecard_records(periods),
            "metric_kinds": dict(zip(names, kinds)),
            "metric_units": dict(zip(names, units)),
            "group1_label": labels[0],
            "group2_label": labels[1],
            "ci_level": params["ci_level"],
            "correction": params["correction"],
            "correction_scope": params["correction_scope"],
            "strata_col": params["strata_col"],
            "period": params["period"],
            "period_basis": params["period_basis"] if params["period"] else None,
            "warnings": list(dict.fromkeys(str(w.message) for w in caught)),
        }

    # ── HTML export (export_html/render_static: see RetentioneeringWidget) ──────

    _export_label = "A/B Test"

    def _export_data(self, sidebar_open: bool | None = None) -> dict:
        data: dict[str, Any] = {
            "widget_type": "ab_test",
            "result": json.loads(self.result or "{}"),
        }
        for name in self._data_names:
            value = getattr(self, name)
            if name == "metrics":
                value = json.loads(value or "[]")
            elif name == "diff":
                value = json.loads(value) if value else None
            data[name] = value
        for name in ("event_list", "path_cols", "segment_cols", "segment_levels"):
            data[name] = json.loads(getattr(self, name) or "[]")
        data["custom_cols"] = json.loads(self.custom_cols or "{}")
        data["assignment_info"] = self.assignment_info
        data["height"] = self.height
        data["sidebar_open"] = (
            sidebar_open if sidebar_open is not None else self.sidebar_open
        )
        return data


def _assignment_info(assignment) -> str:
    if assignment is None:
        return ""
    if isinstance(assignment, pd.DataFrame) and "assigned_at" in assignment.columns:
        return "arm,assigned_at"
    return "arm"
