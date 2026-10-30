"""Joint path-level bootstrap engine.

One set of replicates per call: paths are resampled *jointly* for every metric, stratum
and period cell, so that ratios of sums (AOV = revenue / orders), relative deltas and
contrasts between periods are all evaluated on the same resampled population.

Every statistic is a ratio of weighted path sums restricted to one arm::

    value(arm, cell) = sum_i w_i * num[i, cell] / sum_i w_i * den[i, cell]

A mean is ``den = 1`` for the paths of the cell (0 elsewhere), a proportion has a 0/1
``num``, a ratio metric (average order value) has ``den = orders``.

Engine (lead decision D12)
--------------------------
* Weights per replicate are drawn **within arm**: multinomial (default: each arm keeps its
  size) or Poisson(1).
* Replicates are produced in batches of ``batch`` (default 200) so memory stays
  ``batch x paths``; results are identical for any batch size (``rtol=1e-12`` in tests).
* ``random_state`` seeds ``numpy.random.SeedSequence(random_state).spawn(2)`` — baseline
  (group 2) gets child 0, comparison (group 1) child 1 — so the first ``B``
  replicates of a longer run equal a shorter run, and a cell's replicates do not
  depend on which other cells are requested.
* The caller sorts rows by path id, so results do not depend on the order of events in
  the source frame.
* A cell whose weighted denominator is 0 in a replicate is NaN in that replicate;
  ``valid`` counts the replicates where both arms are finite.

This module has no dependency on :class:`~retentioneering.eventstream.Eventstream`.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

from retentioneering.exceptions import InvalidParameterError
from retentioneering.stats.primitives import stable_sample_summary
from retentioneering.stats.resampling import (
    BOOTSTRAP_ALGORITHM,
    draw_sums,
    draw_weights,
    percentile_interval,
)

__all__ = [
    "MIN_REPLICATES",
    "WEIGHTS",
    "BootstrapResult",
    "bootstrap_p",
    "bootstrap_paths",
    "contrast_rows",
    "percentile_ci",
]

#: Resampling schemes of :func:`bootstrap_paths`.
WEIGHTS = ("multinomial", "poisson")

#: Minimum requested draws for inference; every draw must also be finite.
MIN_REPLICATES = 100


@dataclass
class BootstrapResult:
    """Replicate statistics for every cell.

    Attributes
    ----------
    group1, group2 : ndarray of shape (n_bootstrap, n_cells)
        Per-replicate value of the metric in each arm (ratio of weighted sums); NaN where
        the weighted denominator is 0.
    valid : ndarray of shape (n_cells,)
        Number of replicates where both arms are finite.
    n_bootstrap : int
        Number of replicates.
    weights : str
        ``"multinomial"`` or ``"poisson"``.
    algorithm : str
        Versioned draw algorithm, including the baseline-first seed convention.
    """

    group1: np.ndarray
    group2: np.ndarray
    valid: np.ndarray
    n_bootstrap: int
    weights: str
    algorithm: str = BOOTSTRAP_ALGORITHM

    @property
    def delta(self) -> np.ndarray:
        """Replicate deltas ``group1 - group2`` of shape (n_bootstrap, n_cells)."""
        return self.group1 - self.group2


def _draw(rng: np.random.Generator, weights: str, b: int, n: int) -> np.ndarray:
    """Compatibility wrapper for the common whole-unit weight kernel."""
    return draw_weights(rng, weights, b, n)


def bootstrap_paths(
    num: np.ndarray,
    den: np.ndarray | None,
    arm: np.ndarray,
    n_bootstrap: int = 1000,
    weights: str = "multinomial",
    batch: int = 200,
    random_state: int = 42,
) -> BootstrapResult:
    """Resample paths within each arm and evaluate ratio-of-sums cells.

    Parameters
    ----------
    num : ndarray of shape (n_paths, n_cells)
        Numerator per path and cell (0 for paths outside a stratum/period cell). A
        non-finite entry is treated as "no value": it contributes 0 to the numerator
        and 0 to the denominator of its cell.
    den : ndarray of shape (n_paths, n_cells) or None
        Denominator per path and cell; ``None`` means 1 for every path.
    arm : ndarray of shape (n_paths,)
        0 for group 1, 1 for group 2; other values are ignored.
    n_bootstrap : int, default 1000
        Number of replicates (>= 1).
    weights : {"multinomial", "poisson"}, default "multinomial"
        ``"multinomial"``: each replicate resamples ``n_arm`` paths with replacement
        within each arm (arm sizes fixed). ``"poisson"``: independent Poisson(1)
        weight per path (arm sizes vary).
    batch : int, default 200
        Replicates per batch (memory ``batch x n_paths``); does not change the result.
    random_state : int, default 42
        Seed of ``numpy.random.SeedSequence``; one spawned child per arm.

    Returns
    -------
    BootstrapResult

    Raises
    ------
    InvalidParameterError
        Unknown ``weights``; ``n_bootstrap`` or ``batch`` below 1; shapes that disagree.
    """
    if weights not in WEIGHTS:
        raise InvalidParameterError("bootstrap_weights", weights, list(WEIGHTS))
    if isinstance(n_bootstrap, bool) or not isinstance(n_bootstrap, (int, np.integer)):
        raise InvalidParameterError("n_bootstrap", n_bootstrap, ["an int >= 1"])
    if n_bootstrap < 1:
        raise InvalidParameterError("n_bootstrap", n_bootstrap, ["an int >= 1"])
    if int(batch) < 1:
        raise InvalidParameterError("batch", batch, ["an int >= 1"])
    num = np.asarray(num, dtype=np.float64)
    if num.ndim == 1:
        num = num[:, None]
    den = np.ones_like(num) if den is None else np.asarray(den, dtype=np.float64)
    if den.ndim == 1:
        den = den[:, None]
    arm = np.asarray(arm)
    if den.shape != num.shape or arm.shape != (num.shape[0],):
        raise InvalidParameterError(
            "num/den/arm",
            f"{num.shape}/{den.shape}/{arm.shape}",
            ["num and den of shape (n_paths, n_cells), arm of shape (n_paths,)"],
        )
    missing = ~np.isfinite(num) | ~np.isfinite(den)
    if missing.any():
        num = np.where(missing, 0.0, num)
        den = np.where(missing, 0.0, den)

    n_cells = num.shape[1]
    # Baseline/group 2 gets child 0, comparison/group 1 child 1, matching the
    # strict experiment adapter's control-first sampling contract.
    children = np.random.SeedSequence(random_state).spawn(2)[::-1]
    out = []
    for g, child in enumerate(children):
        rng = np.random.default_rng(child)
        mask = arm == g
        stacked = np.hstack([num[mask], den[mask]])
        n = int(mask.sum())
        values = np.full((n_bootstrap, n_cells), np.nan)
        if n > 0:
            sums = draw_sums(stacked, n_bootstrap, int(batch), rng, weights)
            top, bottom = sums[:, :n_cells], sums[:, n_cells:]
            with np.errstate(invalid="ignore", divide="ignore"):
                values = np.where(bottom > 0, top / bottom, np.nan)
        out.append(values)
    valid = (np.isfinite(out[0]) & np.isfinite(out[1])).sum(axis=0)
    return BootstrapResult(out[0], out[1], valid, int(n_bootstrap), weights)


def percentile_ci(
    replicates: np.ndarray, ci_level: float = 0.95
) -> tuple[np.ndarray, np.ndarray]:
    """Pointwise percentile intervals; every requested draw must be defined.

    Returns ``(low, high)`` arrays. A column with any nonfinite draw, fewer than
    :data:`MIN_REPLICATES` requested draws, or no empirical spread is unavailable.
    Invalid denominator draws are not removed to condition on a selected subset.
    """
    reps = np.asarray(replicates, dtype=np.float64)
    if reps.ndim == 1:
        reps = reps[:, None]
    low = np.full(reps.shape[1], np.nan)
    high = np.full(reps.shape[1], np.nan)
    for column in range(reps.shape[1]):
        low[column], high[column], _, _ = percentile_interval(
            reps[:, column], ci_level, min_replicates=MIN_REPLICATES
        )
    return low, high


def bootstrap_se(replicates: np.ndarray) -> np.ndarray:
    """Standard deviation (ddof=1) when every requested replica is finite.

    Nonfinite denominator draws are not discarded. At least MIN_REPLICATES
    draws are required; constants have zero spread and no bootstrap p-value.
    """
    reps = np.asarray(replicates, dtype=np.float64)
    if reps.ndim == 1:
        reps = reps[:, None]
    finite = np.isfinite(reps).sum(axis=0)
    se = np.full(reps.shape[1], np.nan)
    ok = (finite == len(reps)) & (finite >= MIN_REPLICATES)
    for column in np.flatnonzero(ok):
        _, se[column], _, _ = stable_sample_summary(reps[:, column])
    return se


def bootstrap_p(delta: np.ndarray, replicates: np.ndarray) -> np.ndarray:
    """Two-sided p-value from the bootstrap standard error: ``2 * Phi(-|delta| / sd*)``.

    ``delta`` is the point estimate per column, ``sd*`` the standard deviation of the
    replicate deltas. NaN if any requested replicate is nonfinite, ``sd* == 0``,
    ``delta`` is NaN, or fewer than :data:`MIN_REPLICATES` draws were requested.
    The normal approximation has no ``1 / B`` floor, unlike a percentile p-value.
    """
    se = bootstrap_se(replicates)
    d = np.atleast_1d(np.asarray(delta, dtype=np.float64))
    with np.errstate(invalid="ignore", divide="ignore"):
        z = np.where(se > 0, np.abs(d) / se, np.nan)
    return 2 * stats.norm.sf(z)


def contrast_rows(
    result: BootstrapResult, cells: pd.DataFrame, contrast: dict
) -> pd.DataFrame:
    """Build per-replicate contrast rows.

    Parameters
    ----------
    result : BootstrapResult
        Replicates whose columns include the pooled sides of every contrast.
    cells : pandas.DataFrame
        One row per column of ``result`` (same order) with columns ``metric``,
        ``contrast``, ``side`` (``"a"`` / ``"b"`` on pooled-side columns, NaN elsewhere)
        and optionally ``stratum``. A pooled side is the ratio of the summed numerators
        over the summed denominators of the listed periods.
    contrast : dict
        ``{"name": (periods_a, periods_b)}``; for each metric (and stratum) the contrast
        is ``side a - side b`` within each arm, and ``delta = group1 - group2`` of that.

    Returns
    -------
    pandas.DataFrame
        Columns ``replicate, metric, [stratum], contrast, group1_value, group2_value,
        delta``; NaN where a replicate has no support on either side.
    """
    has_stratum = "stratum" in cells.columns
    keys = ["metric"] + (["stratum"] if has_stratum else [])
    frames = []
    replicate = np.arange(result.n_bootstrap)
    side_cells = cells[cells["side"].notna()] if "side" in cells.columns else cells[:0]
    for name in contrast:
        part = side_cells[side_cells["contrast"] == name]
        for key, group in part.groupby(keys, sort=False, dropna=False):
            key = key if isinstance(key, tuple) else (key,)
            a = group.index[group["side"] == "a"]
            b = group.index[group["side"] == "b"]
            if len(a) != 1 or len(b) != 1:
                continue
            ia, ib = cells.index.get_loc(a[0]), cells.index.get_loc(b[0])
            g1 = result.group1[:, ia] - result.group1[:, ib]
            g2 = result.group2[:, ia] - result.group2[:, ib]
            frame = pd.DataFrame({"replicate": replicate})
            for col, value in zip(keys, key):
                frame[col] = value
            frame["contrast"] = name
            frame["group1_value"] = g1
            frame["group2_value"] = g2
            frame["delta"] = g1 - g2
            frames.append(frame)
    columns = (
        ["replicate"] + keys + ["contrast", "group1_value", "group2_value", "delta"]
    )
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)[columns]
