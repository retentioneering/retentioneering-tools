"""A/B transitions widget: the `get_ab_transitions` screen as a source x target matrix.

Every cell is one row of `Eventstream.ab_transitions_data` (the widget's headless
twin, an alias of `get_ab_transitions`): its background is the delta, its text the
delta and the interval, its outline whether the delta is significantly non-zero
after the correction. See `widgets/_ab_matrix.py` for the payload.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import traitlets

from retentioneering.widgets._ab_matrix import VIEW_DEFAULTS, ABMatrixWidget

_DEFAULTS: dict[str, Any] = {
    "edge_weight": "unique_paths",
    "horizon": "",
    "path_col": "",
    "edges": "",
    "test": "auto",
    "correction": "bh",
    "ci_level": 0.95,
    "power": 0.8,
    "n_bootstrap": 1000,
    "bootstrap_weights": "multinomial",
    "min_support": 10,
    "random_state": 42,
}


class ABTransitionsWidget(ABMatrixWidget):
    widget_type = traitlets.Unicode("ab_transitions").tag(sync=True)

    # ── data parameters (the arguments of ab_transitions_data) ─────────────────
    edge_weight = traitlets.Unicode("unique_paths").tag(sync=True)
    # "" | JSON list of [source, target] pairs
    edges = traitlets.Unicode("").tag(sync=True)
    test = traitlets.Unicode("auto").tag(sync=True)
    n_bootstrap = traitlets.Int(1000).tag(sync=True)
    bootstrap_weights = traitlets.Unicode("multinomial").tag(sync=True)
    random_state = traitlets.Int(42).tag(sync=True)

    # ── view ───────────────────────────────────────────────────────────────────
    #: the reading hint of the conditional weights (rows or columns sum to 100 %)
    reading_hint = traitlets.Bool(True).tag(sync=True)

    _defaults = _DEFAULTS
    _editable = ("edge_weight", "correction", "ci_level", "min_support")
    _view_defaults = {**VIEW_DEFAULTS, "reading_hint": True}
    _tool = "get_ab_transitions"
    _export_label = "A/B Transitions"

    def _to_trait(self, name: str, value: Any) -> Any:
        if name == "edges":
            if value is None or value == "":
                return ""
            if isinstance(value, str):
                return value
            return json.dumps([list(pair) for pair in value], default=str)
        return super()._to_trait(name, value)

    def _params(self, overrides: dict | None = None) -> dict:
        params = super()._params(overrides)
        edges = params["edges"]
        params["edges"] = (
            [tuple(pair) for pair in json.loads(edges)]
            if isinstance(edges, str) and edges
            else (edges or None)
        )
        return params

    def _twin(self, params: dict) -> pd.DataFrame:
        return self._eventstream.ab_transitions_data(self._diff, **params)

    def _binary(self, params: dict) -> bool:
        return params["edge_weight"] == "unique_paths"

    def _payload_extra(self, frame: pd.DataFrame, params: dict) -> dict:
        weight = (frame.attrs or {}).get("edge_weight", params["edge_weight"])
        return {
            "edge_weight": weight,
            "unit": "per_path" if weight == "avg_per_path" else "pp",
            "conditional": weight in ("proba_out", "proba_in", "share_of_total"),
            "edges": (frame.attrs or {}).get("edges", "all"),
        }

    def _tool_ab_transitions_data(self, params: dict):
        return self._compute_raw(self._params(params))

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {"ab_transitions_data": _tool_ab_transitions_data}
