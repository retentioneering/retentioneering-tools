"""How much data an A/B screen needs: ``get_ab_transitions_power`` / ``get_ab_steps_power``.

A screen with many events is data-hungry: every edge (cell) is its own test, its
family is every testable row, and a row with few supporting paths is not tested at
all. These getters project one screen to other sample sizes: for each row and each
``scale`` (a multiple of the current number of paths, the same in both arms) the
paths per arm, the pooled support, whether the row would pass the support rule,
whether it could then be tested, the family it would belong to and the minimum
detectable effect at that family's Bonferroni level.

Design (ADR-0017, amendment "Data sufficiency and vocabulary compression")
--------------------------------------------------------------------------
* The projection is the screen's own machinery with the paths multiplied by
  ``scale``: the pooled support of the support rule scales with the paths, the
  standard error of the delta with ``1 / sqrt(scale)``. A 0/1 row's ``mde`` is
  :func:`~retentioneering.tools._stats.mde_proportion` at the pooled share and the
  projected arm sizes; a Welch or bootstrap row's is
  :func:`~retentioneering.tools._stats.mde_from_se` of its ``delta_se`` divided by
  ``sqrt(scale)``. At ``scale=1`` every number is the getter's.
* Support is not inference. ``supported`` says whether a row passes the support
  rule at a scale; ``inference`` (the same at every scale, :data:`INFERENCE`) says
  whether more paths of the same composition can give it a test: a constant row
  (``"no_spread"``) or one without an estimate (``"no_estimate"``) never can, a
  rare row whose spread the getter has not estimated (``"unknown_spread"``) may or
  may not. ``testable`` is True, NA (unknown) or False accordingly.
* The family at a scale is the number of rows counted in it there
  (:data:`FAMILY_INFERENCE`: supported and tested, projectable or of unknown
  spread), and the level of ``mde`` is ``alpha / m`` (``mde_alpha_of``): the
  Bonferroni level, which is conservative for Benjamini-Hochberg (BH rejects at
  ``alpha * k / m`` for the ``k``-th smallest p-value, never below ``alpha / m``).
  Rows of unknown spread count, so the level stays conservative; rows that can
  never be tested do not.
* Every MDE is separable: ``mde(s) = F(m(s)) * base / sqrt(s)``, with ``F(m)`` the
  normal factor ``z(1 - alpha_m / 2) + z(power)`` of
  :func:`~retentioneering.tools._stats.mde_proportion` /
  :func:`~retentioneering.tools._stats.mde_from_se` at the family's level and
  ``base`` the row's standard error at ``scale=1`` (``sqrt(p (1 - p) (1/n1 +
  1/n2))`` for a z-test row, ``delta_se`` otherwise). The family is a step function
  of the scale (:func:`family_steps`), so the size a row needs for a target
  (:func:`solve_scale_needed`) is solved over its breakpoints: at the size it
  returns, the projected family is the family there ("projected family" sizing).
* Projections assume the same path composition: more paths of the same mix of
  behaviours, the same pooled shares and the same per-path spread.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from scipy import stats

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools.ab_test import mde_alpha_of

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "DEFAULT_SCALE",
    "FAMILY_INFERENCE",
    "INFERENCE",
    "POWER_COLUMNS",
    "SIZING_BASIS",
    "FamilySteps",
    "Projection",
    "check_scale",
    "check_target_delta",
    "family_steps",
    "solve_scale_needed",
    "projection_inputs",
    "power_frame",
    "power_summary",
    "ab_transitions_power",
    "ab_steps_power",
]

#: Multiples of the current number of paths projected by default.
DEFAULT_SCALE: tuple[float, ...] = (0.25, 0.5, 1, 2, 4, 8, 16)

#: What more paths of the same composition can do for a row (``inference``):
#: ``"tested"`` (an adjusted p-value now), ``"projectable"`` (not tested now, a
#: spread known to exist: a z-test row, or a finite positive ``delta_se``),
#: ``"unknown_spread"`` (not tested now, no spread estimated now and not provably
#: constant), ``"no_spread"`` (provably constant) and ``"no_estimate"`` (no test
#: for another reason that more paths of the same mix keep).
INFERENCE: tuple[str, ...] = (
    "tested",
    "projectable",
    "unknown_spread",
    "no_spread",
    "no_estimate",
)
#: The statuses counted in the projected family once a row passes the support rule.
FAMILY_INFERENCE: tuple[str, ...] = ("tested", "projectable", "unknown_spread")
#: The statuses whose MDE is projected (they have a base spread).
SPREAD_INFERENCE: tuple[str, ...] = ("tested", "projectable")

#: ``attrs["sizing_basis"]`` of a power frame, and what it means.
SIZING_BASIS = "projected_family"
SIZING_TEXT = (
    "scale_needed is the smallest multiple of the current paths (a total, not "
    "additional) from which the row's mde, at the Bonferroni level of the "
    "family projected at each size, stays at or below target_delta"
)
FAMILY_TEXT = (
    "family_size counts the rows that pass the support rule at that scale and are "
    "tested, projectable or of unknown spread (counted, so the level stays "
    "conservative); rows with no spread or no estimate never join it"
)

#: Columns after a screen's key columns (optional ones dropped when absent).
POWER_COLUMNS: tuple[str, ...] = (
    "scale",
    "group1_paths",
    "group2_paths",
    "pooled_support",
    "supported",
    "inference",
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

#: Edge weights whose per-path value lies in [0, 1] (a 0/1 indicator, or a ratio
#: of sums whose numerator never exceeds its denominator).
_BOUNDED_WEIGHTS = ("unique_paths", "proba_out", "proba_in", "share_of_total")


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


# ======================================================================= the family
@dataclass(frozen=True)
class FamilySteps:
    """The projected family as a step function of the scale.

    On ``[breakpoints[k], breakpoints[k + 1])`` (the last interval open-ended) the
    family has ``sizes[k]`` rows and a row's MDE is ``factors[k] * base /
    sqrt(scale)``. ``breakpoints[0]`` is 0, with an empty family; the others are
    the distinct ``scale_to_test`` of the rows counted in it, in increasing order.
    """

    breakpoints: np.ndarray
    sizes: np.ndarray
    factors: np.ndarray

    def index_at(self, scale: Any) -> np.ndarray:
        """The interval holding each ``scale``."""
        return np.searchsorted(self.breakpoints, scale, side="right") - 1

    def size_at(self, scale: Any) -> np.ndarray:
        """The projected family ``m(scale)``."""
        return self.sizes[self.index_at(scale)]

    def factor_at(self, scale: Any) -> np.ndarray:
        """``F(m(scale))``: the normal factor of the MDE at the family's level."""
        return self.factors[self.index_at(scale)]

    def payload(self) -> dict:
        """The step function as JSON (the "data needed" view solves with it)."""
        return {
            "breakpoints": [float(b) for b in self.breakpoints],
            "sizes": [int(m) for m in self.sizes],
            "factors": [float(f) for f in self.factors],
            "factor_now": float(self.factor_at(1.0)),
        }


def _factors(sizes: np.ndarray, alpha: float, correction: str, power: float):
    """``_stats._mde_factor`` at ``mde_alpha_of(alpha, m, correction)`` for every
    family size ``m``, vectorised (the same operations, so the same values)."""
    _stats._mde_factor(alpha, power)  # validates both levels
    levels = np.array(
        [mde_alpha_of(alpha, int(m), correction) for m in sizes], dtype=float
    )
    return np.asarray(
        stats.norm.ppf(1 - levels / 2) + stats.norm.ppf(power), dtype=float
    )


def family_steps(
    scale_to_test: Any,
    *,
    alpha: float = 0.05,
    correction: str = "bh",
    power: float = 0.8,
) -> FamilySteps:
    """The family of a screen at every size, as a step function of the scale.

    Parameters
    ----------
    scale_to_test : array-like of float
        Per row of the screen, the multiple of the current paths at which it
        joins the family (passes the support rule, being tested, projectable or
        of unknown spread); NaN or infinite for a row that never does. A tested
        row's value is at most 1.
    alpha, correction, power
        The screen's level, correction and power: the MDE of a family of ``m``
        rows is at ``mde_alpha_of(alpha, m, correction)``.

    Returns
    -------
    FamilySteps
        ``m(s) = #{rows: scale_to_test <= s}`` and its MDE factor on each interval.
    """
    values = np.asarray(scale_to_test, dtype=float).ravel()
    values = np.sort(values[np.isfinite(values)])
    breakpoints = np.unique(np.concatenate([[0.0], np.maximum(values, 0.0)]))
    sizes = np.searchsorted(values, breakpoints, side="right").astype(np.int64)
    alpha = round(float(alpha), 12)
    return FamilySteps(breakpoints, sizes, _factors(sizes, alpha, correction, power))


def solve_scale_needed(
    base: Any, scale_to_test: Any, steps: FamilySteps, target_delta: float
) -> np.ndarray:
    """The multiple of the current paths each row needs for its MDE to reach
    ``target_delta``, with the family projected at that size.

    A row's MDE at scale ``s`` is ``F(m(s)) * base / sqrt(s)`` (``steps``). The
    answer is the smallest ``s >= scale_to_test`` from which that MDE stays at
    most ``target_delta``: on an interval of ``steps`` where the family is constant
    the MDE falls with ``s``, and at a breakpoint the family grows and the MDE can
    jump back above the target, so a size that reaches the target just before a
    breakpoint is not returned when a larger one misses it. It is a total, not
    additional, multiple; at it, ``power_frame(..., scale=[s])`` has the family
    ``m(s)`` and an ``mde`` at most ``target_delta``.

    With ``x = (base / target_delta)^2`` and ``g_k = breakpoints[k] /
    factors[k]^2``, the interval ``k`` misses the target somewhere when ``x *
    factors[k]^2`` exceeds its lower end: for the intervals above the row's own,
    when ``x > g_k``, and the last such interval is found on the suffix minimum
    of ``g``; the answer is then ``min(x * factors[k]^2, breakpoints[k + 1])``,
    or ``scale_to_test`` when no interval misses it. ``scaleNeeded`` in
    ``js/widget/src/ab_matrix_encoding.ts`` is the same arithmetic.

    Parameters
    ----------
    base : array-like of float
        Per row, the standard error of the delta at ``scale=1`` (NaN: no spread to
        project, the answer is NaN).
    scale_to_test : array-like of float
        Per row, the multiple at which it passes the support rule (NaN: never).
    steps : FamilySteps
        The projected family (:func:`family_steps`).
    target_delta : float
        The effect to detect, in the screen's units.

    Returns
    -------
    numpy.ndarray
        Per row, the multiple needed; NaN without a base spread or a finite
        ``scale_to_test``.
    """
    base = np.asarray(base, dtype=float)
    to_test = np.asarray(scale_to_test, dtype=float)
    out = np.full(base.shape, np.nan)
    with np.errstate(invalid="ignore"):
        ok = np.isfinite(base) & (base > 0) & np.isfinite(to_test) & (to_test >= 0)
    if not ok.any():
        return out
    ratio = base[ok] / float(target_delta)
    x = ratio * ratio
    stt = to_test[ok]
    breaks = steps.breakpoints
    squared = steps.factors * steps.factors
    upper = np.append(breaks[1:], np.inf)
    g = breaks / squared
    suffix_min = np.minimum.accumulate(g[::-1])[::-1]
    # the last interval whose lower end the target size exceeds (g[0] = 0 < x)
    last = np.searchsorted(suffix_min, x, side="left") - 1
    own = np.searchsorted(breaks, stt, side="right") - 1
    own_misses = x * squared[own] > stt
    worst = np.where(last > own, last, np.where(own_misses, own, -1))
    at = np.maximum(worst, 0)
    out[ok] = np.where(worst >= 0, np.minimum(x * squared[at], upper[at]), stt)
    return out


# ======================================================================= the rows
@dataclass(frozen=True)
class Projection:
    """What the projection reads off a screen's frame, per row in its order."""

    kind: str
    n1: np.ndarray
    n2: np.ndarray
    support: np.ndarray
    non_support: np.ndarray
    #: the multiple of the paths at which the row passes the support rule (inf:
    #: never); a tested row's is at most 1
    support_scale: np.ndarray
    tested: np.ndarray
    inference: np.ndarray
    #: ``support_scale`` where the row joins the family, else NaN
    scale_to_test: np.ndarray
    #: the standard error of the delta at ``scale=1``; NaN without a spread
    base: np.ndarray
    pooled_share: np.ndarray
    is_z: np.ndarray
    steps: FamilySteps


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


def _bounded(frame: pd.DataFrame) -> bool:
    """Whether every per-path value lies in [0, 1] (a step cell, a 0/1 edge or a
    ratio of sums whose numerator never exceeds its denominator)."""
    if "group1_edge_paths" not in frame.columns:
        return True
    return (frame.attrs or {}).get("edge_weight") in _BOUNDED_WEIGHTS


def _column(frame: pd.DataFrame, name: str) -> np.ndarray:
    if name in frame.columns:
        return frame[name].to_numpy(dtype=float)
    return np.full(len(frame), np.nan)


def projection_inputs(
    frame: pd.DataFrame, *, power: float = 0.8, min_support: int = 10
) -> Projection:
    """Per row of a ``get_ab_transitions`` / ``get_ab_steps`` frame, its support,
    its ``inference`` status, its ``scale_to_test``, its base spread and the
    projected family (the inputs of :func:`power_frame` and of the "data needed"
    view)."""
    meta = dict(frame.attrs or {})
    alpha = float(meta.get("alpha", 0.05))
    correction = str(meta.get("correction", "bh"))
    kind = _kind(frame)
    n_rows = len(frame)
    n1 = _column(frame, "group1_paths")
    n2 = _column(frame, "group2_paths")
    s1, s2 = _supports(frame) if n_rows else (np.zeros(0), np.zeros(0))
    support = s1 + s2
    non_support = (n1 - s1) + (n2 - s2)
    tests = frame["test"].to_numpy(dtype=object) if n_rows else np.zeros(0, object)
    se = _column(frame, "delta_se")
    g1 = _column(frame, "group1_value")
    g2 = _column(frame, "group2_value")
    tested = np.isfinite(_column(frame, "p_value_adjusted"))
    is_z = tests == "z_test"
    with np.errstate(invalid="ignore", divide="ignore"):
        pooled_share = support / (n1 + n2)
        # the multiple of the paths at which a row passes the support rule
        need = np.where(support > 0, min_support / support, np.inf)
        if kind == "binary":
            need = np.maximum(
                need, np.where(non_support > 0, min_support / non_support, np.inf)
            )
        need = np.where(tested, np.minimum(need, 1.0), need)
        rule_now = need <= 1.0
        share_ok = (pooled_share > 0) & (pooled_share < 1)
        se_ok = np.isfinite(se) & (se > 0)
        at_bounds = np.isin(g1, (0.0, 1.0)) & np.isin(g2, (0.0, 1.0))
    # provably constant: a 0/1 row that every path has or none does; a standard
    # error of 0; a value in [0, 1] at 0 or 1 in each arm (a ratio of sums at 1
    # means every path's own ratio is 1, so a resample cannot move it)
    constant = np.where(is_z, ~share_ok, (se == 0) | (_bounded(frame) & at_bounds))
    no_value = ~(np.isfinite(g1) & np.isfinite(g2))
    spread = np.where(is_z, share_ok, se_ok)
    inference = np.full(n_rows, "unknown_spread", dtype=object)
    inference[spread] = "projectable"
    # an arm without a value (no path of it has the denominator) keeps none
    inference[no_value] = "no_estimate"
    inference[rule_now] = "no_estimate"
    inference[constant] = "no_spread"
    inference[tested] = "tested"
    counted = np.isin(inference, FAMILY_INFERENCE)
    scale_to_test = np.where(counted & np.isfinite(need), need, np.nan)
    with np.errstate(invalid="ignore", divide="ignore"):
        base_z = np.sqrt(pooled_share * (1 - pooled_share) * (1 / n1 + 1 / n2))
    base = np.where(is_z, base_z, se)
    base = np.where(np.isin(inference, SPREAD_INFERENCE), base, np.nan)
    steps = family_steps(
        scale_to_test, alpha=alpha, correction=correction, power=float(power)
    )
    return Projection(
        kind=kind,
        n1=n1,
        n2=n2,
        support=support,
        non_support=non_support,
        support_scale=need,
        tested=tested,
        inference=inference,
        scale_to_test=scale_to_test,
        base=base,
        pooled_share=pooled_share,
        is_z=is_z,
        steps=steps,
    )


# ======================================================================= the frame
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
    ``power``, ``min_support``, ``family``, ``sizing_basis``, ``sizing``,
    ``assumption`` and ``curve`` (:func:`power_summary`).
    """
    scales = check_scale(scale)
    target = check_target_delta(target_delta)
    meta = dict(frame.attrs or {})
    alpha = float(meta.get("alpha", 0.05))
    correction = str(meta.get("correction", "bh"))
    n_rows = len(frame)
    rows = projection_inputs(frame, power=power, min_support=min_support)
    n1, n2 = rows.n1, rows.n2
    tests = frame["test"].to_numpy(dtype=object) if n_rows else np.zeros(0, object)
    se = _column(frame, "delta_se")
    g2 = _column(frame, "group2_value")
    spread = np.isin(rows.inference, SPREAD_INFERENCE)
    unknown = rows.inference == "unknown_spread"

    def mde_at(factor: float, level: float) -> np.ndarray:
        if not n_rows:
            return np.zeros(0)
        z = _stats.mde_proportion(
            rows.pooled_share, n1 * factor, n2 * factor, level, power
        )
        other = _stats.mde_from_se(se / math.sqrt(factor), level, power)
        return np.where(
            rows.is_z, np.asarray(z, dtype=float), np.asarray(other, dtype=float)
        )

    if target is not None:
        needed = solve_scale_needed(rows.base, rows.scale_to_test, rows.steps, target)
        paths1_needed = _ceil_paths(n1 * needed)
        paths2_needed = _ceil_paths(n2 * needed)
    parts = []
    for factor in scales:
        p1, p2 = np.rint(n1 * factor), np.rint(n2 * factor)
        supported = rows.support_scale <= factor
        # the rows counted in the family here: supported & counted, by construction
        m = int(rows.steps.size_at(factor))
        level = mde_alpha_of(round(alpha, 12), m, correction)
        mde = mde_at(factor, level)
        with np.errstate(invalid="ignore", divide="ignore"):
            rel_mde = np.where(g2 > 0, mde / g2, np.nan)
        # True: testable; NA: supported, but whether it has a spread is unknown
        testable = pd.arrays.BooleanArray(supported & spread, supported & unknown)
        part = frame[list(keys)].copy()
        part["scale"] = factor
        part["group1_paths"] = p1.astype(np.int64)
        part["group2_paths"] = p2.astype(np.int64)
        part["pooled_support"] = rows.support * factor
        part["supported"] = supported
        part["inference"] = rows.inference
        part["testable"] = testable
        part["family_size"] = m
        part["mde_alpha"] = level
        part["mde"] = mde
        part["rel_mde"] = rel_mde
        part["delta"] = frame["delta"].to_numpy(dtype=float) if n_rows else []
        part["test"] = tests
        part["scale_to_test"] = rows.scale_to_test
        if target is not None:
            part["group1_paths_needed"] = paths1_needed
            part["group2_paths_needed"] = paths2_needed
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
        family=FAMILY_TEXT,
        sizing_basis=SIZING_BASIS,
        sizing=SIZING_TEXT,
        curve=power_summary(out, target),
        assumption=(
            "the same path composition at every scale: the same pooled shares and "
            "per-path spread, more paths"
        ),
    )
    out.attrs = meta
    return out


def _ceil_paths(values: np.ndarray) -> Any:
    finite = np.isfinite(values)
    out = np.where(finite, np.ceil(np.where(finite, values, 0) - 1e-9), 0)
    return pd.array(
        [int(v) if ok else pd.NA for v, ok in zip(out, finite)], dtype="Int64"
    )


def power_summary(frame: pd.DataFrame, target: float | None) -> list[dict]:
    """``attrs["curve"]``: per scale, the paths, the cells, the cells that pass the
    support rule (``supported``) and those of them of unknown spread
    (``unknown_spread``), the testable cells, the family, the median and quartiles
    of the testable cells' MDE and, with a target, the share of testable cells
    whose MDE reaches it; ``no_spread`` and ``no_estimate`` count the cells that no
    scale makes testable for those reasons (the same at every scale)."""
    curve = []
    if not len(frame):
        return curve
    for factor, part in frame.groupby("scale", sort=True):
        testable = part["testable"].fillna(False).to_numpy(dtype=bool)
        supported = part["supported"].to_numpy(dtype=bool)
        inference = part["inference"].to_numpy(dtype=object)
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
            "supported": int(supported.sum()),
            "unknown_spread": int((supported & (inference == "unknown_spread")).sum()),
            "no_spread": int((inference == "no_spread").sum()),
            "no_estimate": int((inference == "no_estimate").sum()),
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
