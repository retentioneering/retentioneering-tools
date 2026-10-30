"""How much data an A/B screen needs: ``get_ab_transitions_power`` / ``get_ab_steps_power``.

A screen with many events is data-hungry: every edge (cell) is its own test, its
family is every testable row, and a row with few supporting paths is not tested at
all. These getters project one screen to other sample sizes: for each row and each
``scale`` (a multiple of the current number of paths, the same in both arms) the
paths per arm, the pooled support, whether the row would pass the support rule, the
family it would belong to and the minimum detectable effect at that family's
Bonferroni level.

Design (ADR-0017, amendment "Data sufficiency and vocabulary compression")
--------------------------------------------------------------------------
* The projection is the screen's own machinery with the paths multiplied by
  ``scale``: the pooled support of the support rule scales with the paths, the
  standard error of the delta with ``1 / sqrt(scale)``. A 0/1 row's ``mde`` is
  :func:`~retentioneering.tools._stats.mde_proportion` at the pooled share and the
  projected arm sizes; a Welch or bootstrap row's is
  :func:`~retentioneering.tools._stats.mde_from_se` of its ``delta_se`` divided by
  ``sqrt(scale)``. At ``scale=1`` every number is the getter's.
* The family at a scale is the number of rows that would be tested there, and the
  level of ``mde`` is ``alpha / m`` (``mde_alpha_of``): the Bonferroni level, which
  is conservative for Benjamini-Hochberg (BH rejects at ``alpha * k / m`` for the
  ``k``-th smallest p-value, never below ``alpha / m``).
* Projections assume the same path composition: more paths of the same mix of
  behaviours, the same pooled shares and the same per-path spread.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools.ab_test import mde_alpha_of

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "DEFAULT_SCALE",
    "POWER_COLUMNS",
    "check_scale",
    "check_target_delta",
    "power_frame",
    "power_summary",
    "ab_transitions_power",
    "ab_steps_power",
]

#: Multiples of the current number of paths projected by default.
DEFAULT_SCALE: tuple[float, ...] = (0.25, 0.5, 1, 2, 4, 8, 16)

#: Columns after a screen's key columns (optional ones dropped when absent).
POWER_COLUMNS: tuple[str, ...] = (
    "scale",
    "group1_paths",
    "group2_paths",
    "pooled_support",
    "testable",
    "family_size",
    "mde_alpha",
    "mde",
    "rel_mde",
    "delta",
    "test",
    "scale_to_test",
    "group1_paths_needed",
    "group2_paths_needed",
    "scale_needed",
)


def check_scale(scale: Any) -> list[float]:
    """``scale`` as a sorted list of distinct positive floats that contains 1."""
    allowed = ["a non-empty list of positive numbers, e.g. (0.25, 0.5, 1, 2, 4)"]
    if isinstance(scale, (int, float)) and not isinstance(scale, bool):
        scale = [scale]
    if isinstance(scale, (str, bytes, dict)) or not hasattr(scale, "__iter__"):
        raise InvalidParameterError("scale", scale, allowed)
    values = []
    for value in scale:
        if isinstance(value, bool) or not isinstance(value, (int, float, np.number)):
            raise InvalidParameterError("scale", value, allowed)
        value = float(value)
        if not math.isfinite(value) or value <= 0:
            raise InvalidParameterError("scale", value, allowed)
        values.append(value)
    if not values:
        raise InvalidParameterError("scale", scale, allowed)
    values = sorted(set(values) | {1.0})
    return values


def check_target_delta(target_delta: Any) -> float | None:
    """``None`` or a positive finite float."""
    if target_delta is None:
        return None
    if isinstance(target_delta, bool) or not isinstance(
        target_delta, (int, float, np.number)
    ):
        raise InvalidParameterError(
            "target_delta", target_delta, ["None", "a positive number"]
        )
    value = float(target_delta)
    if not math.isfinite(value) or value <= 0:
        raise InvalidParameterError(
            "target_delta", target_delta, ["None", "a positive number"]
        )
    return value


def _supports(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Supporting paths per arm of every row (what the support rule counts)."""
    if "group1_edge_paths" in frame.columns:
        s1 = frame["group1_edge_paths"].to_numpy(dtype=float)
        s2 = frame["group2_edge_paths"].to_numpy(dtype=float)
    else:  # a step cell: the arm's share of its paths
        s1 = np.rint(
            frame["group1_value"].to_numpy(dtype=float)
            * frame["group1_paths"].to_numpy(dtype=float)
        )
        s2 = np.rint(
            frame["group2_value"].to_numpy(dtype=float)
            * frame["group2_paths"].to_numpy(dtype=float)
        )
    return np.nan_to_num(s1), np.nan_to_num(s2)


def _kind(frame: pd.DataFrame) -> str:
    """The support rule's kind: ``"binary"`` for 0/1 rows, else ``"other"``."""
    tests = set(pd.unique(frame["test"])) if len(frame) else set()
    if "group1_edge_paths" not in frame.columns:
        return "binary"
    weight = (frame.attrs or {}).get("edge_weight")
    if weight == "unique_paths" or tests == {"z_test"}:
        return "binary"
    return "other"


def power_frame(
    frame: pd.DataFrame,
    keys: tuple[str, ...],
    *,
    scale: Any = DEFAULT_SCALE,
    target_delta: Any = None,
    power: float = 0.8,
    min_support: int = 10,
) -> pd.DataFrame:
    """Project a ``get_ab_transitions`` / ``get_ab_steps`` frame to other sizes.

    One row per (row of ``frame``, ``scale``); see ``Eventstream.get_ab_transitions_power``
    for the columns. ``attrs`` are the frame's plus ``scale``, ``target_delta``,
    ``power`` and ``curve`` (:func:`power_summary`).
    """
    scales = check_scale(scale)
    target = check_target_delta(target_delta)
    meta = dict(frame.attrs or {})
    alpha = float(meta.get("alpha", 0.05))
    correction = str(meta.get("correction", "bh"))
    kind = _kind(frame)
    n_rows = len(frame)
    n1 = frame["group1_paths"].to_numpy(dtype=float) if n_rows else np.zeros(0)
    n2 = frame["group2_paths"].to_numpy(dtype=float) if n_rows else np.zeros(0)
    s1, s2 = _supports(frame)
    tests = frame["test"].to_numpy(dtype=object) if n_rows else np.zeros(0, object)
    se = frame["delta_se"].to_numpy(dtype=float) if n_rows else np.zeros(0)
    g2 = frame["group2_value"].to_numpy(dtype=float) if n_rows else np.zeros(0)
    tested_now = (
        np.isfinite(frame["p_value_adjusted"].to_numpy(dtype=float))
        if n_rows
        else np.zeros(0, dtype=bool)
    )
    rule_now = (
        _rule(kind, s1 + s2, (n1 - s1) + (n2 - s2), min_support)
        if n_rows
        else np.zeros(0, dtype=bool)
    )
    # a row that passes the support rule and is still untested has no spread or no
    # estimate: more paths of the same composition do not change that
    blocked = rule_now & ~tested_now
    with np.errstate(invalid="ignore", divide="ignore"):
        pooled_share = (s1 + s2) / (n1 + n2)
    is_z = tests == "z_test"

    def mde_at(factor: float, level: float) -> np.ndarray:
        if not n_rows:
            return np.zeros(0)
        z = _stats.mde_proportion(pooled_share, n1 * factor, n2 * factor, level, power)
        other = _stats.mde_from_se(se / math.sqrt(factor), level, power)
        return np.where(
            is_z, np.asarray(z, dtype=float), np.asarray(other, dtype=float)
        )

    # the multiple of the paths at which a row passes the support rule
    with np.errstate(invalid="ignore", divide="ignore"):
        need = np.where(s1 + s2 > 0, min_support / (s1 + s2), np.inf)
        if kind == "binary":
            non = (n1 - s1) + (n2 - s2)
            need = np.maximum(need, np.where(non > 0, min_support / non, np.inf))
    scale_to_test = np.where(blocked | ~np.isfinite(need), np.nan, need)

    parts = []
    for factor in scales:
        p1, p2 = np.rint(n1 * factor), np.rint(n2 * factor)
        if factor == 1.0:
            testable = tested_now.copy()
        else:
            testable = (
                _rule(
                    kind,
                    (s1 + s2) * factor,
                    ((n1 - s1) + (n2 - s2)) * factor,
                    min_support,
                )
                & ~blocked
            )
        m = int(testable.sum())
        level = mde_alpha_of(round(alpha, 12), m, correction)
        mde = mde_at(factor, level)
        with np.errstate(invalid="ignore", divide="ignore"):
            rel_mde = np.where(g2 > 0, mde / g2, np.nan)
        part = frame[list(keys)].copy()
        part["scale"] = factor
        part["group1_paths"] = p1.astype(np.int64)
        part["group2_paths"] = p2.astype(np.int64)
        part["pooled_support"] = (s1 + s2) * factor
        part["testable"] = testable
        part["family_size"] = m
        part["mde_alpha"] = level
        part["mde"] = mde
        part["rel_mde"] = rel_mde
        part["delta"] = frame["delta"].to_numpy(dtype=float) if n_rows else []
        part["test"] = tests
        part["scale_to_test"] = scale_to_test
        if target is not None:
            with np.errstate(invalid="ignore", divide="ignore"):
                needed = (mde / target) ** 2 * factor
            needed = np.where(np.isfinite(mde) & (mde > 0), needed, np.nan)
            part["group1_paths_needed"] = _ceil_paths(n1 * needed)
            part["group2_paths_needed"] = _ceil_paths(n2 * needed)
            part["scale_needed"] = needed
        parts.append(part)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if len(out):
        out = out.sort_values(list(keys) + ["scale"], kind="stable").reset_index(
            drop=True
        )
    else:
        out = pd.DataFrame(columns=list(keys) + list(POWER_COLUMNS))
    present = list(keys) + [c for c in POWER_COLUMNS if c in out.columns]
    out = out[present]
    meta.update(
        tool=f"{meta.get('tool', '')}_power",
        scale=list(scales),
        target_delta=target,
        power=float(power),
        min_support=int(min_support),
        curve=power_summary(out, target),
        assumption=(
            "the same path composition at every scale: the same pooled shares and "
            "per-path spread, more paths"
        ),
    )
    out.attrs = meta
    return out


def _rule(kind: str, support, non_support, min_support: int) -> np.ndarray:
    """The pooled support rule of ``support_rule`` on (possibly projected) pooled
    counts: at least ``min_support`` supporting paths and, for 0/1 rows, as many
    paths without the behaviour."""
    support = np.asarray(support, dtype=float)
    ok = (support >= min_support - 1e-9) & (support > 0)
    if kind == "binary":
        ok &= np.asarray(non_support, dtype=float) >= min_support - 1e-9
    return ok


def _ceil_paths(values: np.ndarray) -> Any:
    finite = np.isfinite(values)
    out = np.where(finite, np.ceil(np.where(finite, values, 0) - 1e-9), 0)
    return pd.array(
        [int(v) if ok else pd.NA for v, ok in zip(out, finite)], dtype="Int64"
    )


def power_summary(frame: pd.DataFrame, target: float | None) -> list[dict]:
    """``attrs["curve"]``: per scale, the paths, the cells, the testable cells, the
    median and quartiles of their MDE and, with a target, the share of testable
    cells whose MDE reaches it."""
    curve = []
    if not len(frame):
        return curve
    for factor, part in frame.groupby("scale", sort=True):
        testable = part["testable"].to_numpy(dtype=bool)
        mde = part["mde"].to_numpy(dtype=float)[testable]
        mde = mde[np.isfinite(mde)]
        entry = {
            "scale": float(factor),
            "group1_paths": int(part["group1_paths"].iloc[0]),
            "group2_paths": int(part["group2_paths"].iloc[0]),
            "cells": int(len(part)),
            "testable": int(testable.sum()),
            "family_size": int(part["family_size"].iloc[0]),
            "mde_alpha": float(part["mde_alpha"].iloc[0]),
            "mde_median": float(np.median(mde)) if mde.size else None,
            "mde_q25": float(np.quantile(mde, 0.25)) if mde.size else None,
            "mde_q75": float(np.quantile(mde, 0.75)) if mde.size else None,
        }
        if target is not None:
            entry["share_reaching_target"] = (
                float((mde <= target).mean()) if mde.size else None
            )
        curve.append(entry)
    return curve


def ab_transitions_power(
    stream: "Eventstream",
    diff: Any,
    edge_weight: str = "unique_paths",
    *,
    scale: Any = DEFAULT_SCALE,
    target_delta: Any = None,
    **getter_args: Any,
) -> pd.DataFrame:
    """The table of ``Eventstream.get_ab_transitions_power`` (see its docstring)."""
    from retentioneering.tools.ab_transitions import ab_transitions

    check_scale(scale)
    check_target_delta(target_delta)
    frame = ab_transitions(stream, diff, edge_weight, **getter_args)
    return power_frame(
        frame,
        ("source", "target"),
        scale=scale,
        target_delta=target_delta,
        power=getter_args.get("power", 0.8),
        min_support=getter_args.get("min_support", 10),
    )


def ab_steps_power(
    stream: "Eventstream",
    diff: Any,
    max_steps: int = 10,
    *,
    scale: Any = DEFAULT_SCALE,
    target_delta: Any = None,
    **getter_args: Any,
) -> pd.DataFrame:
    """The table of ``Eventstream.get_ab_steps_power`` (see its docstring)."""
    from retentioneering.tools.ab_transitions import ab_steps

    check_scale(scale)
    check_target_delta(target_delta)
    frame = ab_steps(stream, diff, max_steps, **getter_args)
    return power_frame(
        frame,
        ("event", "step"),
        scale=scale,
        target_delta=target_delta,
        power=getter_args.get("power", 0.8),
        min_support=getter_args.get("min_support", 10),
    )
