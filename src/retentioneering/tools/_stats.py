"""Pure statistical primitives for the A/B testing tools.

This module has no dependency on :class:`~retentioneering.eventstream.Eventstream`.
Every function accepts scalars or NumPy arrays, broadcasts, and propagates NaN
instead of raising: an absent estimate is never 0. Formulas and golden values
are listed next to each function so that tests can be checked by hand.

Conventions
-----------
* ``ci_level`` is the two-sided confidence level in (0, 1), e.g. 0.95.
* ``k`` and ``n`` are successes and trials; ``n == 0`` yields NaN.
* Intervals are returned as ``(low, high)`` tuples of arrays (0-d inputs give
  NumPy scalars).
* ``delta`` is always ``group1 - group2``.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from retentioneering.exceptions import InvalidParameterError

__all__ = [
    "wilson_ci",
    "newcombe_ci",
    "two_proportion_z",
    "welch_t",
    "delta_log_ratio_ci",
    "mde_proportion",
    "mde_mean",
    "mde_from_se",
    "holm",
    "bonferroni",
    "cochran_q",
    "chi2_balance",
    "srm_chi2",
    "exact_binomial_ci",
]


def _check_level(name: str, value: float) -> float:
    """Validate a probability-like argument strictly inside (0, 1)."""
    try:
        level = float(value)
    except (TypeError, ValueError):
        raise InvalidParameterError(name, value, ["a float in (0, 1)"]) from None
    if not 0.0 < level < 1.0:
        raise InvalidParameterError(name, value, ["a float in (0, 1)"])
    return level


def _z(ci_level: float) -> float:
    """Two-sided standard normal quantile for ``ci_level``."""
    return float(stats.norm.ppf(0.5 + _check_level("ci_level", ci_level) / 2))


def _floats(*arrays):
    return tuple(np.asarray(a, dtype=float) for a in arrays)


def _out(x):
    """Return NumPy scalars for 0-d results, arrays otherwise."""
    x = np.asarray(x, dtype=float)
    return x[()] if x.ndim == 0 else x


# ------------------------------------------------------------------ proportions
def wilson_ci(k, n, ci_level: float = 0.95) -> tuple[np.ndarray, np.ndarray]:
    """Wilson score interval for a binomial proportion.

    Parameters
    ----------
    k : int or array-like
        Successes.
    n : int or array-like
        Trials. ``n == 0`` gives ``(nan, nan)``.
    ci_level : float, default 0.95
        Two-sided confidence level.

    Returns
    -------
    tuple of ndarray
        ``(low, high)``.

    Notes
    -----
    Golden value: ``wilson_ci(37, 120)`` = ``(0.23272736988916298, 0.39582997723800684)``
    (Newcombe 1998, Table I, method 3).
    """
    k, n = _floats(k, n)
    z = _z(ci_level)
    with np.errstate(invalid="ignore", divide="ignore"):
        p = k / n
        den = 1 + z**2 / n
        centre = (p + z**2 / (2 * n)) / den
        half = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / den
        # The bounds are exactly 0 at k = 0 and 1 at k = n; set them so rounding
        # does not leave 0.9999999999999999.
        low = np.where(k <= 0, 0.0, np.clip(centre - half, 0.0, 1.0))
        high = np.where(k >= n, 1.0, np.clip(centre + half, 0.0, 1.0))
        low = np.where(n > 0, low, np.nan)
        high = np.where(n > 0, high, np.nan)
    return _out(low), _out(high)


def newcombe_ci(
    k1, n1, k2, n2, ci_level: float = 0.95
) -> tuple[np.ndarray, np.ndarray]:
    """Newcombe hybrid-score interval for ``p1 - p2`` (method 10, 1998).

    Parameters
    ----------
    k1, n1 : int or array-like
        Successes and trials of group 1.
    k2, n2 : int or array-like
        Successes and trials of group 2.
    ci_level : float, default 0.95
        Two-sided confidence level.

    Returns
    -------
    tuple of ndarray
        ``(low, high)`` of the difference ``k1/n1 - k2/n2``; NaN when either ``n`` is 0.

    Notes
    -----
    Golden value: ``newcombe_ci(56, 70, 48, 80)`` = ``(0.0524, 0.3339)`` to four decimals
    (Newcombe 1998, example (a)).
    """
    k1, n1, k2, n2 = _floats(k1, n1, k2, n2)
    l1, u1 = wilson_ci(k1, n1, ci_level)
    l2, u2 = wilson_ci(k2, n2, ci_level)
    with np.errstate(invalid="ignore", divide="ignore"):
        p1, p2 = k1 / n1, k2 / n2
        d = p1 - p2
        low = d - np.sqrt((p1 - l1) ** 2 + (u2 - p2) ** 2)
        high = d + np.sqrt((u1 - p1) ** 2 + (p2 - l2) ** 2)
    return _out(low), _out(high)


def two_proportion_z(k1, n1, k2, n2) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Pooled two-proportion z-test.

    Parameters
    ----------
    k1, n1, k2, n2 : int or array-like
        Successes and trials of group 1 and group 2.

    Returns
    -------
    tuple of ndarray
        ``(z, p_value, se_unpooled)``. ``p_value`` is NaN when the pooled proportion is
        0 or 1 (both arms constant). ``se_unpooled`` is
        ``sqrt(p1 (1 - p1) / n1 + p2 (1 - p2) / n2)``, the standard error reported with the
        delta. Golden value: ``two_proportion_z(56, 70, 48, 80)[1]`` =
        ``0.00804508136819656``.
    """
    k1, n1, k2, n2 = _floats(k1, n1, k2, n2)
    with np.errstate(invalid="ignore", divide="ignore"):
        p1, p2 = k1 / n1, k2 / n2
        pooled = (k1 + k2) / (n1 + n2)
        se0 = np.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
        z = np.where(se0 > 0, (p1 - p2) / se0, np.nan)
        se = np.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
        se = np.where((n1 > 0) & (n2 > 0), se, np.nan)
    p = 2 * stats.norm.sf(np.abs(z))
    return _out(z), _out(p), _out(se)


def delta_log_ratio_ci(
    k1, n1, k2, n2, ci_level: float = 0.95
) -> tuple[np.ndarray, np.ndarray]:
    """Confidence interval for the relative difference ``p1/p2 - 1`` of two proportions.

    Uses the delta method on ``log(p1/p2)``:
    ``var = (1 - p1) / k1 + (1 - p2) / k2`` (the Katz log interval for a risk ratio).
    Returns ``(nan, nan)`` when ``k1 == 0`` or ``k2 == 0`` (the log ratio is not finite).
    Only valid for 0/1 metrics; ratio and value metrics use the bootstrap percentile of
    the ratio instead (see :mod:`retentioneering.tools._bootstrap`).

    Returns
    -------
    tuple of ndarray
        ``(low, high)`` of ``p1/p2 - 1``.
    """
    k1, n1, k2, n2 = _floats(k1, n1, k2, n2)
    z = _z(ci_level)
    with np.errstate(invalid="ignore", divide="ignore"):
        p1, p2 = k1 / n1, k2 / n2
        ok = (k1 > 0) & (k2 > 0) & (n1 > 0) & (n2 > 0)
        log_ratio = np.log(p1 / p2)
        half = z * np.sqrt((1 - p1) / k1 + (1 - p2) / k2)
        low = np.where(ok, np.expm1(log_ratio - half), np.nan)
        high = np.where(ok, np.expm1(log_ratio + half), np.nan)
    return _out(low), _out(high)


# ------------------------------------------------------------------ means
def welch_t(
    mean1, var1, n1, mean2, var2, n2
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Welch's unequal-variance t-test on summary statistics.

    Parameters
    ----------
    mean1, var1, n1 : float or array-like
        Mean, sample variance (``ddof=1``) and size of group 1.
    mean2, var2, n2 : float or array-like
        The same for group 2.

    Returns
    -------
    tuple of ndarray
        ``(t, df, p_value, se)`` for ``mean1 - mean2``. ``se`` is
        ``sqrt(var1 / n1 + var2 / n2)`` and ``df`` the Welch-Satterthwaite degrees of
        freedom. All four are NaN when ``n1 < 2`` or ``n2 < 2`` or both variances are 0
        (no estimate of the sampling variability).
    """
    mean1, var1, n1, mean2, var2, n2 = _floats(mean1, var1, n1, mean2, var2, n2)
    with np.errstate(invalid="ignore", divide="ignore"):
        v1, v2 = var1 / n1, var2 / n2
        se = np.sqrt(v1 + v2)
        ok = (n1 >= 2) & (n2 >= 2) & (se > 0)
        df = (v1 + v2) ** 2 / (v1**2 / (n1 - 1) + v2**2 / (n2 - 1))
        t = (mean1 - mean2) / se
        se = np.where(ok, se, np.nan)
        df = np.where(ok, df, np.nan)
        t = np.where(ok, t, np.nan)
    p = 2 * stats.t.sf(np.abs(t), df)
    return _out(t), _out(df), _out(p), _out(se)


# ------------------------------------------------------------------ MDE
def _mde_factor(alpha: float, power: float) -> float:
    alpha = _check_level("alpha", alpha)
    power = _check_level("power", power)
    return float(stats.norm.ppf(1 - alpha / 2) + stats.norm.ppf(power))


def mde_proportion(p, n1, n2, alpha: float = 0.05, power: float = 0.8) -> np.ndarray:
    """Minimum detectable absolute difference for two proportions (normal approximation).

    ``(z_{1-alpha/2} + z_{power}) * sqrt(p (1 - p) (1/n1 + 1/n2))``. ``p`` is the pooled
    baseline rate; ``alpha`` is the per-comparison level after multiplicity correction
    (``alpha_used`` in the scorecard). NaN when ``p`` is not in (0, 1) or an ``n`` is 0.
    """
    p, n1, n2 = _floats(p, n1, n2)
    factor = _mde_factor(alpha, power)
    with np.errstate(invalid="ignore", divide="ignore"):
        ok = (p > 0) & (p < 1) & (n1 > 0) & (n2 > 0)
        mde = factor * np.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    return _out(np.where(ok, mde, np.nan))


def mde_mean(sd, n1, n2, alpha: float = 0.05, power: float = 0.8) -> np.ndarray:
    """Minimum detectable difference of means for a given pooled standard deviation.

    ``(z_{1-alpha/2} + z_{power}) * sd * sqrt(1/n1 + 1/n2)``; NaN when an ``n`` is 0.
    """
    sd, n1, n2 = _floats(sd, n1, n2)
    factor = _mde_factor(alpha, power)
    with np.errstate(invalid="ignore", divide="ignore"):
        mde = factor * sd * np.sqrt(1 / n1 + 1 / n2)
    return _out(np.where((n1 > 0) & (n2 > 0), mde, np.nan))


def mde_from_se(se, alpha: float = 0.05, power: float = 0.8) -> np.ndarray:
    """Minimum detectable effect from a standard error of the delta.

    ``(z_{1-alpha/2} + z_{power}) * se``. Used for bootstrap cells where ``se`` is the
    bootstrap standard deviation of the delta.
    """
    (se,) = _floats(se)
    return _out(_mde_factor(alpha, power) * se)


# ------------------------------------------------------------------ multiplicity
def holm(p) -> np.ndarray:
    """Holm step-down adjusted p-values.

    NaN entries are ignored (``m`` = number of finite p-values) and returned as NaN.
    Adjusted values are clipped to 1 and made monotone; ties keep their input order.
    """
    p = np.atleast_1d(np.asarray(p, dtype=float))
    out = np.full_like(p, np.nan)
    ok = np.isfinite(p)
    q = p[ok]
    m = q.size
    if m == 0:
        return out
    order = np.argsort(q, kind="mergesort")
    adjusted = np.minimum(1.0, np.maximum.accumulate((m - np.arange(m)) * q[order]))
    result = np.empty(m)
    result[order] = adjusted
    out[ok] = result
    return out


def bonferroni(p) -> np.ndarray:
    """Bonferroni adjusted p-values with the same NaN rule as :func:`holm`."""
    p = np.atleast_1d(np.asarray(p, dtype=float))
    ok = np.isfinite(p)
    out = np.full_like(p, np.nan)
    out[ok] = np.minimum(1.0, p[ok] * int(ok.sum()))
    return out


# ------------------------------------------------------------------ heterogeneity, chi-square
def cochran_q(deltas, ses, support) -> tuple[float, float, int, int]:
    """Cochran's Q test of homogeneity of per-stratum deltas.

    ``Q = sum w_i (d_i - d_bar)^2`` with ``w_i = 1 / se_i^2`` and the inverse-variance
    weighted mean ``d_bar``; ``Q ~ chi2(df = strata_used - 1)`` under homogeneity.

    Parameters
    ----------
    deltas, ses : array-like
        Per-stratum delta and its standard error.
    support : array-like of bool
        Whether the stratum satisfies the support guard (>= 5 events and >= 5 non-events
        per arm for 0/1 metrics; >= 10 paths per arm for means). Strata with a non-finite
        delta or a standard error that is not positive are not used either.

    Returns
    -------
    tuple
        ``(q, p_value, df, strata_used)``; ``(nan, nan, 0, strata_used)`` when fewer than two
        strata pass the guard.
    """
    d, s = _floats(deltas, ses)
    d, s = np.atleast_1d(d), np.atleast_1d(s)
    sup = np.atleast_1d(np.asarray(support, dtype=bool))
    with np.errstate(invalid="ignore"):
        ok = sup & np.isfinite(d) & np.isfinite(s) & (s > 0)
    used = int(ok.sum())
    if used < 2:
        return float("nan"), float("nan"), 0, used
    w = 1.0 / s[ok] ** 2
    d_bar = float((w * d[ok]).sum() / w.sum())
    q = float((w * (d[ok] - d_bar) ** 2).sum())
    df = used - 1
    return q, float(stats.chi2.sf(q, df)), df, used


def chi2_balance(table) -> tuple[float, float, int]:
    """Chi-square test of independence on a strata x arm contingency table.

    Pearson statistic without continuity correction; rows and columns with a zero
    total are dropped first. Returns ``(chi2, p_value, dof)``; ``(nan, nan, 0)`` when
    fewer than two rows or two columns remain. Documented as a *share balance* check
    only: a balanced share does not make a post-exposure stratum a valid conditioning
    variable.
    """
    t = np.asarray(table, dtype=float)
    if t.ndim != 2:
        raise InvalidParameterError("table", t.shape, ["a 2-D strata x arm table"])
    t = t[t.sum(axis=1) > 0][:, t.sum(axis=0) > 0]
    if t.shape[0] < 2 or t.shape[1] < 2:
        return float("nan"), float("nan"), 0
    expected = t.sum(axis=1, keepdims=True) * t.sum(axis=0, keepdims=True) / t.sum()
    chi2 = float(((t - expected) ** 2 / expected).sum())
    dof = (t.shape[0] - 1) * (t.shape[1] - 1)
    return chi2, float(stats.chi2.sf(chi2, dof)), dof


def srm_chi2(counts, expected_shares=None) -> tuple[float, float, int]:
    """Chi-square goodness-of-fit for sample ratio mismatch.

    ``counts`` are paths per arm; ``expected_shares`` sum to 1 within 1e-9 (equal split
    when None). Returns ``(chi2, p_value, dof)`` with ``dof = len(counts) - 1``;
    ``(nan, nan, dof)`` when there are no paths at all. A level with an expected share of
    0 that holds paths gives ``chi2 = inf`` and ``p_value = 0``.

    Raises
    ------
    InvalidParameterError
        Shares of the wrong length, negative, or not summing to 1.
    """
    c = np.atleast_1d(np.asarray(counts, dtype=float))
    k = c.size
    if expected_shares is None:
        e = np.full(k, 1.0 / k) if k else np.array([])
    else:
        e = np.atleast_1d(np.asarray(expected_shares, dtype=float))
        if e.size != k or np.any(e < 0) or not np.isfinite(e).all():
            raise InvalidParameterError(
                "expected_shares", list(e), [f"{k} non-negative shares summing to 1"]
            )
        if abs(e.sum() - 1.0) > 1e-9:
            raise InvalidParameterError(
                "expected_shares", list(e), ["shares summing to 1"]
            )
    dof = max(k - 1, 0)
    total = c.sum()
    if total <= 0 or k < 2:
        return float("nan"), float("nan"), dof
    expected = total * e
    with np.errstate(invalid="ignore", divide="ignore"):
        terms = (c - expected) ** 2 / expected
    terms = np.where((expected == 0) & (c == 0), 0.0, terms)
    chi2 = float(terms.sum())
    return chi2, float(stats.chi2.sf(chi2, dof)), dof


def exact_binomial_ci(k, n, ci_level: float = 0.95) -> tuple[np.ndarray, np.ndarray]:
    """Clopper-Pearson interval; used only in tests as a reference.

    ``low = Beta(alpha/2; k, n - k + 1)``, ``high = Beta(1 - alpha/2; k + 1, n - k)``,
    with ``low = 0`` at ``k = 0`` and ``high = 1`` at ``k = n``; NaN when ``n == 0``.
    """
    k, n = _floats(k, n)
    alpha = 1 - _check_level("ci_level", ci_level)
    with np.errstate(invalid="ignore", divide="ignore"):
        low = np.where(k > 0, stats.beta.ppf(alpha / 2, k, n - k + 1), 0.0)
        high = np.where(k < n, stats.beta.ppf(1 - alpha / 2, k + 1, n - k), 1.0)
    low = np.where(n > 0, low, np.nan)
    high = np.where(n > 0, high, np.nan)
    return _out(low), _out(high)
