"""The A/B screens as tables in the shape of the diff matrices.

``get_ab_transitions_table`` and ``get_ab_steps_table`` pivot the frame of
``get_ab_transitions`` / ``get_ab_steps``: one row per source event (per event),
one column group per target event (per step), and inside each group one column per
requested statistic, ``("delta", "delta_ci_low", "delta_ci_high",
"p_value_adjusted")`` by default. Nothing is recomputed: every cell is a cell of the
frame, a cell without a row in the frame is NaN, and the frame's ``attrs`` travel
with the table. ``style_table`` draws the encoding of the ``ab_transitions`` /
``ab_steps`` widgets with a pandas ``Styler``, in CSS that ``Styler.to_excel`` keeps.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidParameterError

__all__ = [
    "TABLE_VALUES",
    "KEYS",
    "matrix_axes",
    "cell_status",
    "ab_table",
    "style_table",
]

#: The statistics of every cell, in this order, by default.
TABLE_VALUES = ("delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted")

#: Row and column key of each screen.
KEYS = {
    "get_ab_transitions": ("source", "target"),
    "get_ab_steps": ("event", "step"),
}

#: Colours of the encoding, shared with the widgets (js/widget/src/ab_matrix_encoding.ts):
#: red where group 1 is higher, blue where it is lower (the diff palette).
RED = (239, 68, 68)
BLUE = (59, 130, 246)
#: Largest opacity of a cell background (|delta| at the scale maximum).
MAX_ALPHA = 0.75
SIGNIFICANT_BORDER = "#111827"
MUTED_BORDER = "#d1d5db"
NOT_SIGNIFICANT_TEXT = "#6b7280"
UNTESTED_TEXT = "#9ca3af"


def _event_order(events) -> list:
    """``path_start`` first, ``path_end`` last, every other event by name."""
    start, end = EventTypes().PATH_START.name, EventTypes().PATH_END.name
    present = set(events)
    middle = sorted((e for e in present if e not in (start, end)), key=str)
    return (
        ([start] if start in present else [])
        + middle
        + ([end] if end in present else [])
    )


def matrix_axes(frame: pd.DataFrame) -> tuple[list, list]:
    """The rows and columns of a screen's matrix: the events (sources) that have a
    row in the frame, ``path_start`` first, ``path_end`` last and the rest by name,
    and the targets in the same order, or the steps in ascending order."""
    row_key, col_key = _keys_of(frame)
    if col_key == "step":
        return _event_order(frame["event"]), sorted({int(s) for s in frame["step"]})
    return _event_order(frame["source"]), _event_order(frame["target"])


def _keys_of(frame: pd.DataFrame) -> tuple[str, str]:
    tool = (frame.attrs or {}).get("tool")
    if tool in KEYS:
        return KEYS[tool]
    return (
        KEYS["get_ab_steps"] if "step" in frame.columns else KEYS["get_ab_transitions"]
    )


def check_values(values: Any, frame: pd.DataFrame) -> list[str]:
    """``values`` as a list of the frame's statistic columns, or a clear error."""
    keys = _keys_of(frame)
    allowed = [c for c in frame.columns if c not in keys]
    hint = [f"a non-empty list of the frame's columns: {', '.join(allowed)}"]
    if isinstance(values, str):
        values = [values]
    if isinstance(values, (bytes, dict)) or not hasattr(values, "__iter__"):
        raise InvalidParameterError("values", values, hint)
    picked = list(values)
    if not picked:
        raise InvalidParameterError("values", values, hint)
    for name in picked:
        if not isinstance(name, str) or name not in allowed:
            reason = (
                f"'{name}' is a key of the table (its rows and columns)"
                if name in keys
                else None
            )
            raise InvalidParameterError("values", name, allowed, reason=reason)
    if len(set(picked)) != len(picked):
        duplicated = sorted({n for n in picked if picked.count(n) > 1})
        raise InvalidParameterError(
            "values", picked, hint, reason=f"{duplicated} appear more than once"
        )
    return picked


def ab_table(frame: pd.DataFrame, values: Any = TABLE_VALUES) -> pd.DataFrame:
    """Pivot a ``get_ab_transitions`` / ``get_ab_steps`` frame into the diff matrix shape.

    Index: the sources (events); columns: a ``(target, stat)`` (``(step, stat)``)
    MultiIndex with ``values`` in the given order inside each target (step). The
    frame's values unchanged, NaN where the frame has no row; ``attrs`` are the
    frame's plus ``values``.
    """
    picked = check_values(values, frame)
    row_key, col_key = _keys_of(frame)
    rows, cols = matrix_axes(frame)
    columns = pd.MultiIndex.from_product([cols, picked], names=[col_key, "stat"])
    if len(frame):
        wide = frame.set_index([row_key, col_key])[picked].unstack(col_key)
        wide = wide.swaplevel(0, 1, axis=1)
        wide.columns = wide.columns.set_names([col_key, "stat"])
        table = wide.reindex(index=rows, columns=columns)
    else:
        table = pd.DataFrame(index=pd.Index(rows), columns=columns, dtype=float)
    table.index.name = row_key
    table.attrs = {**dict(frame.attrs or {}), "values": list(picked)}
    return table


def cell_status(row: dict | None, alpha: float) -> str:
    """How the widgets draw a cell: ``"significant"`` (adjusted p-value below
    ``alpha``: black outline), ``"not_significant"`` (tested: grey outline and a
    grey cross), ``"untested"`` (a row without an adjusted p-value: dashed grey
    outline, cross, muted text) or ``"empty"`` (no row: the transition or the event
    at that step never occurs in the compared paths)."""
    if row is None:
        return "empty"
    p = row.get("p_value_adjusted")
    try:
        p = float(p)
    except (TypeError, ValueError):
        return "untested"
    if not math.isfinite(p):
        return "untested"
    return "significant" if p < alpha else "not_significant"


def _blend(rgb: tuple[int, int, int], alpha: float) -> str:
    """``rgb`` at opacity ``alpha`` over white, as an opaque hex colour (Excel has
    no transparency)."""
    channel = [round(255 + (c - 255) * alpha) for c in rgb]
    return "#{:02x}{:02x}{:02x}".format(*channel)


def delta_background(delta: Any, scale: float) -> str | None:
    """The background of a delta: red for a positive one, blue for a negative one,
    opacity proportional to ``|delta| / scale`` up to :data:`MAX_ALPHA`."""
    try:
        delta = float(delta)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(delta) or delta == 0 or not scale > 0:
        return None
    alpha = MAX_ALPHA * min(1.0, abs(delta) / scale)
    return _blend(RED if delta > 0 else BLUE, alpha)


def style_table(table: pd.DataFrame, frame: pd.DataFrame):
    """A pandas ``Styler`` of ``table`` with the widgets' encoding.

    The ``delta`` column of each group gets the diverging background (scaled to the
    largest ``|delta|`` of the frame); every column of a significant group gets a
    black border around the group and bold text; a tested, not significant group
    grey text and a thin grey border; an untested group light grey italic text and
    a dashed grey border; a group without a row in the frame stays blank. Only CSS
    that ``Styler.to_excel`` translates is used (hex colours, borders, font).
    """
    alpha = float((frame.attrs or {}).get("alpha", 0.05))
    row_key, col_key = _keys_of(frame)
    rows = {
        (r[row_key], r[col_key]): r
        for r in frame[[row_key, col_key, "p_value_adjusted", "delta"]].to_dict(
            "records"
        )
    }
    deltas = frame["delta"].to_numpy(dtype=float)
    finite = np.abs(deltas[np.isfinite(deltas)])
    scale = float(finite.max()) if finite.size else 0.0
    picked = list(table.columns.get_level_values("stat").unique())

    css = pd.DataFrame("", index=table.index, columns=table.columns)
    for source in table.index:
        for target in table.columns.get_level_values(col_key).unique():
            row = rows.get((source, target))
            status = cell_status(row, alpha)
            if status == "empty":
                continue
            if status == "significant":
                border = f"2px solid {SIGNIFICANT_BORDER}"
                font = "font-weight: bold; color: #111827;"
            elif status == "not_significant":
                border = f"1px solid {MUTED_BORDER}"
                font = f"color: {NOT_SIGNIFICANT_TEXT};"
            else:
                border = f"1px dashed {MUTED_BORDER}"
                font = f"color: {UNTESTED_TEXT}; font-style: italic;"
            for pos, stat in enumerate(picked):
                parts = [font, f"border-top: {border};", f"border-bottom: {border};"]
                if pos == 0:
                    parts.append(f"border-left: {border};")
                if pos == len(picked) - 1:
                    parts.append(f"border-right: {border};")
                if stat == "delta":
                    background = delta_background(row["delta"], scale)
                    if background:
                        parts.append(f"background-color: {background};")
                css.loc[source, (target, stat)] = " ".join(parts)

    def _fmt(value: Any, stat: str) -> str:
        if value is None or (isinstance(value, float) and not math.isfinite(value)):
            return ""
        if isinstance(value, (float, np.floating)):
            if stat.startswith("p_value"):
                return f"{value:.2g}"
            return f"{value:.4f}"
        return str(value)

    formatters = {
        column: (lambda v, s=column[1]: _fmt(v, s)) for column in table.columns
    }
    styler = table.style.apply(lambda _: css, axis=None).format(formatters)
    meta = frame.attrs or {}
    styler = styler.set_caption(
        f"{meta.get('group1_label', 'group 1')} − {meta.get('group2_label', 'group 2')}"
        f"; bold black border: adjusted p < {alpha:g}"
        f" ({meta.get('correction', '')} over {meta.get('n_tested', '?')} tested);"
        " grey: not significant; dashed: not tested; blank: never occurs"
    )
    return styler
