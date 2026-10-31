"""Shared base of the A/B difference matrices: ``ab_transitions`` and ``ab_steps``.

Each widget draws one frame of its headless twin (``ab_transitions_data`` /
``ab_steps_data``, documented aliases of ``get_ab_transitions`` / ``get_ab_steps``,
ADR-0006/ADR-0017) as a matrix: one cell per row of the frame. The rows travel as
they come back, every cell with NaN / NA as null (``scorecard_records``), column by
column with repeated values sent once (``encode_frame``: 80 event types are 6,561
rows, each with its ``reading``), plus what the browser cannot read off a row: the
matrix axes, the frame's ``attrs`` and why a row without an adjusted p-value was
not tested. ``decode_payload`` (``parsePayload`` in the browser) gives the rows
back. The cell encoding itself (background, outline, cross) is drawn in
``js/widget/src/ab_matrix.tsx``.

Data parameters are the twin's; the sidebar edits the few that a reader varies
while reading (the weight or the number of steps, the correction, the level and
the support threshold) and recomputes; the others are fixed at construction and
shown read-only. View parameters (intervals shown, significance filter, colour
scale, sorting, hidden and pinned events, cell size, the "Delta | Data needed"
switch and its ``target_delta``) live in the browser and are saved with
``state_file``.

The "data needed" view reads ``power`` (``power_payload``): the frame projected
to other sizes by the machinery of ``get_ab_transitions_power`` /
``get_ab_steps_power`` — the curve of testable cells and median MDE against the
users, and per row the multiple of users at which it becomes testable.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
import traitlets

from retentioneering.tools.ab_power import DEFAULT_SCALE, power_frame
from retentioneering.tools.ab_tables import KEYS, matrix_axes
from retentioneering.widgets._ab_widget import ABFrameWidget, jsonable
from retentioneering.widgets.ab_test import scorecard_records

#: View parameters: set in the browser, saved with ``state_file``, never recomputed.
VIEW_DEFAULTS: dict[str, Any] = {
    "show_ci": True,
    # "per_cell" (delta_ci_low / delta_ci_high) or "simultaneous" (the Bonferroni
    # delta_ci_low_adj / delta_ci_high_adj, when the frame has them)
    "interval_kind": "per_cell",
    # "all", "dim" (rows / columns without a significant cell faded) or "hide"
    "significance_filter": "all",
    # largest |delta| of the colour scale in the frame's units; 0 = the largest
    # |delta| among the cells shown
    "color_clip": 0.0,
    # "name", "max_abs_delta" or "n_significant"
    "row_sort": "name",
    "col_sort": "name",
    # "auto", "compact" (delta only; the interval in the tooltip) or "normal"
    "cell_size": "auto",
    # '{"<event>": {"isHidden": bool, "isPinned": bool}}'
    "event_visibility": "{}",
    # "delta" (the difference) or "data_needed" (the MDE, and the users a cell
    # needs: the payload's ``power``)
    "view_mode": "delta",
    # the "data needed" view's goal in the frame's units: a cell shows the
    # multiple of users its MDE needs to reach it; 0 = none
    "target_delta": 0.0,
}


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def untested_reason(row: dict, *, kind: str, min_support: int) -> str | None:
    """Why a row of the frame has no adjusted p-value, read off its own columns
    (``None`` for a tested row).

    ``kind`` is ``"binary"`` (``unique_paths`` and step cells, where the support
    rule also counts the paths without the behaviour) or anything else.
    """
    if _finite(row.get("p_value_adjusted")):
        return None
    warning = row.get("support_warning") or ""
    if warning == "no_data":
        return "nothing to compare: no path of either arm has it, or an arm has no estimate"
    if warning == "invalid_replicates":
        return (
            "more than 1 % of bootstrap replicates had no value in an arm: no "
            "interval or p-value"
        )
    n1, n2 = int(row.get("group1_paths") or 0), int(row.get("group2_paths") or 0)
    if "group1_edge_paths" in row:
        s1 = int(row.get("group1_edge_paths") or 0)
        s2 = int(row.get("group2_edge_paths") or 0)
    else:  # a step cell: the arm's share of its paths
        g1, g2 = row.get("group1_value"), row.get("group2_value")
        s1 = int(round(float(g1) * n1)) if _finite(g1) else 0
        s2 = int(round(float(g2) * n2)) if _finite(g2) else 0
    if s1 + s2 < min_support:
        return f"pooled support {s1 + s2} < min_support {min_support}"
    if kind == "binary" and (n1 - s1) + (n2 - s2) < min_support:
        return (
            f"pooled paths without it {(n1 - s1) + (n2 - s2)} < min_support "
            f"{min_support}"
        )
    return (
        "no p-value: no spread to estimate (the value is constant in both arms, or "
        "an arm has too few paths)"
    )


def encode_column(values: list) -> dict:
    """One column of the payload, losslessly: ``{"values": [...]}`` as it is or,
    when it has at most half as many distinct values as rows, ``{"levels": [...],
    "codes": [...]}`` with ``values[i] == levels[codes[i]]``; a column with one
    value for every row is ``{"levels": [value]}``. See ``decode_column``."""
    levels: list = []
    position: dict = {}
    codes = []
    for value in values:
        # repr keeps 1, True and 1.0 apart, and 0.0 and -0.0
        key = (type(value), repr(value))
        if key not in position:
            position[key] = len(levels)
            levels.append(value)
        codes.append(position[key])
    if not values or 2 * len(levels) > len(values):
        return {"values": list(values)}
    if len(levels) == 1:
        return {"levels": levels}
    return {"levels": levels, "codes": codes}


def decode_column(column: dict, length: int) -> list:
    """The values of an ``encode_column`` column of ``length`` rows."""
    if "values" in column:
        return list(column["values"])
    if "codes" in column:
        return [column["levels"][code] for code in column["codes"]]
    return column["levels"] * length


def encode_frame(rows: list[dict], columns: list[str]) -> dict:
    """The rows of a frame (one dict per row) as ``{"length", "columns": [{"name",
    ...encode_column}]}``, in the frame's column order."""
    return {
        "length": len(rows),
        "columns": [
            {"name": name, **encode_column([row[name] for row in rows])}
            for name in columns
        ],
    }


def decode_payload(payload: dict) -> dict:
    """The widget's ``result`` with ``rows`` (one dict per row of the frame, in its
    column order) in place of ``frame``, and ``untested_reason`` and
    ``power["scale_to_test"]`` as lists: what the browser reads (``parsePayload``
    in ``js/widget/src/ab_matrix_encoding.ts``). An empty result (``{}``) comes
    back as it is."""
    if "frame" not in payload:
        return dict(payload)
    frame = payload["frame"]
    length = frame["length"]
    names = [column["name"] for column in frame["columns"]]
    columns = [decode_column(column, length) for column in frame["columns"]]
    decoded = {key: value for key, value in payload.items() if key != "frame"}
    decoded["rows"] = [dict(zip(names, cells)) for cells in zip(*columns)]
    decoded["untested_reason"] = decode_column(payload["untested_reason"], length)
    if "power" in payload:
        power = dict(payload["power"])
        power["scale_to_test"] = decode_column(power["scale_to_test"], length)
        decoded["power"] = power
    return decoded


def power_payload(frame: pd.DataFrame, keys: tuple[str, ...], params: dict) -> dict:
    """What the "data needed" view draws: the projection of the frame to other
    sizes (``power_frame``, the machinery of ``get_ab_transitions_power`` /
    ``get_ab_steps_power``).

    ``curve`` is ``attrs["curve"]`` (per scale: the testable cells and the median
    MDE); ``scale_to_test`` is, per row of the frame in its order, the multiple of
    the users at which the row passes the support rule (null when more users of
    the same composition would not make it testable). The multiple a row needs
    for a ``target_delta`` is ``(mde / target_delta) ** 2`` at the current
    family's level, which the browser computes (``scale_needed`` of the power
    getters at ``scale=1``)."""
    projected = power_frame(
        frame,
        keys,
        scale=DEFAULT_SCALE,
        power=params["power"],
        min_support=params["min_support"],
    )
    one = projected[projected["scale"] == 1.0]
    if len(frame):
        lookup = one.set_index(list(keys))["scale_to_test"]
        aligned = lookup.reindex(pd.MultiIndex.from_frame(frame[list(keys)]))
        to_test = [
            float(v) if math.isfinite(float(v)) else None for v in aligned.to_numpy()
        ]
    else:
        to_test = []
    attrs = projected.attrs
    return {
        "scale": [float(s) for s in attrs["scale"]],
        "curve": jsonable(attrs["curve"]),
        "scale_to_test": encode_column(to_test),
        "level": "alpha / m (Bonferroni; conservative for Benjamini-Hochberg)",
        "assumption": attrs["assumption"],
    }


class ABMatrixWidget(ABFrameWidget):
    """Common part of ``ABTransitionsWidget`` and ``ABStepsWidget``."""

    # ── view (browser-only, persisted) ─────────────────────────────────────────
    show_ci = traitlets.Bool(True).tag(sync=True)
    interval_kind = traitlets.Unicode("per_cell").tag(sync=True)
    significance_filter = traitlets.Unicode("all").tag(sync=True)
    color_clip = traitlets.Float(0.0).tag(sync=True)
    row_sort = traitlets.Unicode("name").tag(sync=True)
    col_sort = traitlets.Unicode("name").tag(sync=True)
    cell_size = traitlets.Unicode("auto").tag(sync=True)
    event_visibility = traitlets.Unicode("{}").tag(sync=True)
    view_mode = traitlets.Unicode("delta").tag(sync=True)
    target_delta = traitlets.Float(0.0).tag(sync=True)

    _view_defaults: dict[str, Any] = VIEW_DEFAULTS
    #: ``"get_ab_transitions"`` or ``"get_ab_steps"``.
    _tool: str = ""

    def _binary(self, params: dict) -> bool:
        return True

    def _payload(self, frame: pd.DataFrame, params: dict, caught: list[str]) -> dict:
        meta = dict(frame.attrs or {})
        rows = scorecard_records(frame) or []
        kind = "binary" if self._binary(params) else "other"
        rows_axis, cols_axis = matrix_axes(frame)
        return {
            "tool": self._tool,
            "frame": encode_frame(rows, list(frame.columns)),
            "untested_reason": encode_column(
                [
                    untested_reason(row, kind=kind, min_support=params["min_support"])
                    for row in rows
                ]
            ),
            "row_keys": [str(k) for k in rows_axis],
            "col_keys": [
                str(k) if self._tool == "get_ab_transitions" else int(k)
                for k in cols_axis
            ],
            "attrs": jsonable(meta),
            "group1_label": meta.get("group1_label", "group 1"),
            "group2_label": meta.get("group2_label", "group 2"),
            "alpha": meta.get("alpha", round(1 - params["ci_level"], 12)),
            "ci_level": meta.get("ci_level", params["ci_level"]),
            "correction": meta.get("correction", params["correction"]),
            "n_tested": meta.get("n_tested"),
            "n_rows": meta.get("n_rows", len(rows)),
            "population": meta.get("population"),
            "horizon": meta.get("horizon"),
            "path_col": meta.get("path_col"),
            "min_support": params["min_support"],
            "simultaneous": "delta_ci_low_adj" in frame.columns,
            "warnings": list(dict.fromkeys(caught)),
            "power": power_payload(frame, KEYS[self._tool], params),
            **self._payload_extra(frame, params),
        }

    def _payload_extra(self, frame: pd.DataFrame, params: dict) -> dict:
        return {}
