"""Shared statistical primitives for the A/B testing tools.

This module has no dependency on :class:`~retentioneering.eventstream.Eventstream`.
Summary-statistic formulas accept scalars or NumPy arrays and broadcast. Raw
sample helpers preserve the observations needed for exact constancy checks and
stable moments. Undefined estimates are NaN; invalid configuration is rejected.
Formulas and golden values accompany the helpers for independent verification.

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
    "benjamini_hochberg",
    "bonferroni",
    "chi2_balance",
    "cochran_q",
    "constant_statistic",
    "delta_log_ratio_ci",
    "exact_binomial_ci",
    "exact_difference_ci",
    "fisher_exact_p",
    "holm",
    "mde_from_se",
    "mde_mean",
    "mde_proportion",
    "newcombe_ci",
    "paired_constant_statistic",
    "sample_mean_ci",
    "srm_chi2",
    "stable_sample_summary",
    "two_proportion_z",
    "welch_samples",
    "welch_t",
    "wilson_ci",
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
    return float(stats.norm.isf((1.0 - _check_level("ci_level", ci_level)) / 2))


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
    """Asymptotic pooled two-proportion z-test, not an exact small-sample test.

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


def _binomial_counts(k, n):
    k, n = np.broadcast_arrays(*_floats(k, n))
    if not (
        np.isfinite(k).all()
        and np.isfinite(n).all()
        and ((0 <= k) & (k <= n)).all()
        and (k == np.floor(k)).all()
        and (n == np.floor(n)).all()
    ):
        raise ValueError("Binomial counts require finite integers 0 <= k <= n")
    return k, n


def fisher_exact_p(k1, n1, k2, n2):
    """Two-sided conditional Fisher p-value for independent binary groups.

    Sum fixed-margin table probabilities no larger than the observed table's
    probability. Returns NaN for an empty arm and 1 for equal constant arms.
    Inputs broadcast; counts must be finite nonnegative integers with k <= n.
    Independence, fixed observation rules and a prespecified test family remain
    design requirements. This test is not dual to a difference interval.
    """
    k1, n1 = _binomial_counts(k1, n1)
    k2, n2 = _binomial_counts(k2, n2)
    k1, n1, k2, n2 = np.broadcast_arrays(k1, n1, k2, n2)
    if np.any(n1 + n2 >= float(np.iinfo(np.int64).max)):
        raise ValueError("Fisher table total exceeds the supported integer range")
    result = np.full(k1.shape, np.nan)
    for index in np.ndindex(k1.shape):
        if n1[index] > 0 and n2[index] > 0:
            table = [
                [int(k1[index]), int(n1[index] - k1[index])],
                [int(k2[index]), int(n2[index] - k2[index])],
            ]
            result[index] = stats.fisher_exact(table, alternative="two-sided").pvalue
    return _out(result)


def delta_log_ratio_ci(
    k1, n1, k2, n2, ci_level: float = 0.95
) -> tuple[np.ndarray, np.ndarray]:
    """Confidence interval for the relative difference ``p1/p2 - 1`` of two proportions.

    Uses the delta method on ``log(p1/p2)``:
    ``var = (1 - p1) / k1 + (1 - p2) / k2`` (the Katz log interval for a risk ratio).
    Returns ``(nan, nan)`` when ``k1 == 0`` or ``k2 == 0`` (the log ratio is not finite)
    and when ``k1 == n1`` or ``k2 == n2`` (the variance term of that arm is 0, which
    would collapse the interval to a point however small the arm).
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
        ok = (k1 > 0) & (k2 > 0) & (k1 < n1) & (k2 < n2)
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
    with np.errstate(invalid="ignore", divide="ignore", over="ignore", under="ignore"):
        v1, v2 = var1 / n1, var2 / n2
        scale = np.maximum(v1, v2)
        a, b = v1 / scale, v2 / scale
        se = np.sqrt(scale) * np.sqrt(a + b)
        ok = (
            (n1 >= 2)
            & (n2 >= 2)
            & (var1 >= 0)
            & (var2 >= 0)
            & (se > 0)
            & np.isfinite(se)
        )
        df = (a + b) ** 2 / (a**2 / (n1 - 1) + b**2 / (n2 - 1))
        t = (mean1 - mean2) / se
        ok &= np.isfinite(t) & np.isfinite(df)
        se = np.where(ok, se, np.nan)
        df = np.where(ok, df, np.nan)
        t = np.where(ok, t, np.nan)
    p = 2 * stats.t.sf(np.abs(t), df)
    return _out(t), _out(df), _out(p), _out(se)


# ------------------------------------------------------------------ MDE
def _mde_factor(alpha: float, power: float) -> float:
    alpha = _check_level("alpha", alpha)
    power = _check_level("power", power)
    if power < 0.5:
        raise InvalidParameterError("power", power, ["a float in [0.5, 1)"])
    return float(stats.norm.isf(alpha / 2) + stats.norm.ppf(power))


def mde_proportion(p, n1, n2, alpha: float = 0.05, power: float = 0.8) -> np.ndarray:
    """Minimum detectable absolute difference for two proportions (normal approximation).

    ``(z_{1-alpha/2} + z_{power}) * sqrt(p (1 - p) (1/n1 + 1/n2))``. ``p`` is the pooled
    baseline rate; ``alpha`` is the per-comparison level after multiplicity correction
    (``mde_alpha`` in the scorecard). NaN when ``p`` is not in (0, 1) or an ``n`` is 0.
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
    bootstrap standard deviation of the delta. NaN when ``se`` is not positive and
    finite: a zero spread (a constant metric) says nothing about detectable effects.
    """
    (se,) = _floats(se)
    with np.errstate(invalid="ignore"):
        ok = np.isfinite(se) & (se > 0)
    return _out(np.where(ok, _mde_factor(alpha, power) * se, np.nan))


# ------------------------------------------------------------------ multiplicity


def _validate_pvalues(p):
    if np.isinf(p).any() or ((p[np.isfinite(p)] < 0) | (p[np.isfinite(p)] > 1)).any():
        raise ValueError("p-values must be in [0,1] or missing")


def holm(p) -> np.ndarray:
    """Holm step-down adjusted p-values.

    All declared entries count toward the family size, including unavailable
    tests (NaN). Missing entries stay NaN. Values are clipped to 1 and monotone;
    ties retain their input order. This avoids shrinking a prespecified family
    after seeing which tests were estimable.
    """
    p = np.atleast_1d(np.asarray(p, dtype=float))
    _validate_pvalues(p)
    out = np.full_like(p, np.nan)
    ok = np.isfinite(p)
    q = p[ok]
    m = p.size
    if q.size == 0:
        return out
    order = np.argsort(q, kind="mergesort")
    adjusted = np.minimum(
        1.0, np.maximum.accumulate((m - np.arange(q.size)) * q[order])
    )
    result = np.empty(q.size)
    result[order] = adjusted
    out[ok] = result
    return out


def bonferroni(p) -> np.ndarray:
    """Bonferroni adjusted p-values with the same NaN rule as :func:`holm`."""
    p = np.atleast_1d(np.asarray(p, dtype=float))
    _validate_pvalues(p)
    ok = np.isfinite(p)
    out = np.full_like(p, np.nan)
    out[ok] = np.minimum(1.0, p[ok] * p.size)
    return out


def benjamini_hochberg(p) -> np.ndarray:
    """Benjamini-Hochberg step-up adjusted p-values (false discovery rate).

    ``p_adj(i) = min_{j >= i} (m / j) * p_(j)`` over the ascending order, clipped to 1.
    Unavailable entries remain NaN and count toward the declared family size,
    as in :func:`holm`. Controls the false discovery rate, not the family-wise error:
    use it for many exploratory rows (strata, transitions), Holm for a few confirmatory
    ones.
    """
    p = np.atleast_1d(np.asarray(p, dtype=float))
    _validate_pvalues(p)
    out = np.full_like(p, np.nan)
    ok = np.isfinite(p)
    q = p[ok]
    m = p.size
    if q.size == 0:
        return out
    order = np.argsort(q, kind="mergesort")
    scaled = q[order] * m / np.arange(1, q.size + 1)
    adjusted = np.minimum(1.0, np.minimum.accumulate(scaled[::-1])[::-1])
    result = np.empty(q.size)
    result[order] = adjusted
    out[ok] = result
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
    d, s = d[ok], s[ok]
    # Centre before scaling to preserve small differences on a large offset.
    # Inverse raw squared SE overflows/underflows under an innocent unit change.
    with np.errstate(over="ignore", under="ignore", invalid="ignore", divide="ignore"):
        centered = d - d[0]
        if np.isfinite(centered).all():
            scale = max(float(np.max(np.abs(centered))), float(s.max()))
            centered = centered / scale
        else:
            scale = float(np.max(np.abs(d)))
            centered = d / scale - d[0] / scale
        scaled_se = s / scale
        if (scaled_se <= 0).any():
            return float("nan"), float("nan"), used - 1, used
        w = (scaled_se.min() / scaled_se) ** 2
        center_mean = float(np.sum((w / w.sum()) * centered))
        standardized = (centered - center_mean) / scaled_se
        q = float(np.sum(standardized**2))
    df = used - 1
    if not np.isfinite(q):
        return float("nan"), float("nan"), df, used
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
    positive = e > 0
    dof = max(int(positive.sum()) - 1, 0)
    total = c.sum()
    if total > 0 and np.any(c[~positive] > 0):
        return float("inf"), 0.0, dof
    c, e = c[positive], e[positive]
    if total <= 0 or len(c) < 2:
        return float("nan"), float("nan"), dof
    expected = total * e
    with np.errstate(invalid="ignore", divide="ignore"):
        terms = (c - expected) ** 2 / expected
    terms = np.where((expected == 0) & (c == 0), 0.0, terms)
    chi2 = float(terms.sum())
    return chi2, float(stats.chi2.sf(chi2, dof)), dof


def _clopper_pearson_tails(k, n, tail):
    """Clopper-Pearson endpoints using the tail probability directly."""
    k, n = _binomial_counts(k, n)
    with np.errstate(invalid="ignore", divide="ignore"):
        low = np.where(k > 0, stats.beta.ppf(tail, k, n - k + 1), 0.0)
        high = np.where(k < n, stats.beta.isf(tail, k + 1, n - k), 1.0)
    return _out(np.where(n > 0, low, np.nan)), _out(np.where(n > 0, high, np.nan))


def exact_binomial_ci(k, n, ci_level: float = 0.95) -> tuple[np.ndarray, np.ndarray]:
    """Conservative exact Clopper-Pearson interval for a binomial proportion.

    ``low = Beta(alpha/2; k, n - k + 1)``, ``high = Beta(1 - alpha/2; k + 1, n - k)``,
    with ``low = 0`` at ``k = 0`` and ``high = 1`` at ``k = n``; NaN when ``n == 0``.
    """
    alpha = 1 - _check_level("ci_level", ci_level)
    return _clopper_pearson_tails(k, n, alpha / 2)


def exact_difference_ci(k1, n1, k2, n2, ci_level: float = 0.95):
    """Conservative exact interval for p1-p2 from Bonferroni CP arm bounds.

    Each arm interval has error probability at most alpha/2; the union bound
    gives difference coverage at least 1-alpha under binomial sampling. This is
    usually wider than Newcombe and is not obtained by inverting Fisher's test.
    Tail probabilities alpha/4 avoid rounding a derived confidence level to 1.
    """
    tail = (1 - _check_level("ci_level", ci_level)) / 4
    l1, u1 = _clopper_pearson_tails(k1, n1, tail)
    l2, u2 = _clopper_pearson_tails(k2, n2, tail)
    return _out(np.maximum(-1, l1 - u2)), _out(np.minimum(1, u1 - l2))


def constant_statistic(values):
    """Whether every defined empirical resample has the same pair-sum ratio.

    Inspect observations, not noisy sums of resampled decimal values. Equal
    denominators (including means) need exact numerator equality. Unequal
    denominators require division, so permit only a few floating-point rounding
    units in the resulting ratios, with no absolute tolerance near zero.
    """
    numerator, denominator = values[:, 0], values[:, 1]
    positive = denominator > 0
    if not positive.any() or np.any(numerator[~positive] != 0):
        return False
    a, b = numerator[positive], denominator[positive]
    if np.all(b == b[0]):
        return bool(np.all(a == a[0]))
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        ratios = a / b
        return bool(
            np.isfinite(ratios).all()
            and np.isclose(
                ratios, ratios[0], rtol=4 * np.finfo(float).eps, atol=0
            ).all()
        )


def paired_constant_statistic(numerator_a, numerator_b, denominator):
    """Whether a paired change is constant at the resolution of its source values.

    Use only when both sides have the identical per-unit denominator. Unlike a
    raw observation, ``a - b`` carries the resolution of two source operands and
    their subtraction. Exact equality of these derived differences can turn
    decimal representation noise into spurious sampling uncertainty.

    Each source operand and the subtraction receive a bound of machine epsilon
    times their magnitude (with one smallest-subnormal unit at underflow). For a
    varying denominator, propagate that bound through division and include its
    rounding. A common change must lie in every resulting interval. This is an
    operation-scale resolution guard, not a fixed tolerance on the outcome: real
    variation in tiny source values is preserved when it exceeds their own
    floating-point resolution. Nonfinite inputs are not classified as constant.
    """
    a, b, den = (
        np.asarray(x, dtype=float) for x in (numerator_a, numerator_b, denominator)
    )
    if a.ndim != 1 or a.shape != b.shape or a.shape != den.shape:
        raise ValueError(
            "paired contributions must be equal-length one-dimensional arrays"
        )
    if not all(np.isfinite(x).all() for x in (a, b, den)) or (den < 0).any():
        return False
    positive = den > 0
    if not positive.any():
        return False
    with np.errstate(over="ignore", under="ignore", invalid="ignore", divide="ignore"):
        change = a - b
        if not np.isfinite(change).all() or np.any(change[~positive] != 0):
            return False
        a, b, den, change = (x[positive] for x in (a, b, den, change))
        epsilon = np.finfo(float).eps
        subnormal = np.finfo(float).smallest_subnormal

        def resolution(values):
            return np.maximum(epsilon * np.abs(values), subnormal)

        error = resolution(a) + resolution(b) + resolution(change)
        if not np.all(den == den[0]):
            change = change / den
            error = error / den + resolution(change)
        if not np.isfinite(change).all() or not np.isfinite(error).all():
            return False
        return bool(np.max(change - error) <= np.min(change + error))


def stable_sample_summary(values):
    """Mean, sample SD, count and status without squaring raw magnitudes.

    Missing-value eligibility is a caller decision: this function does not drop
    nonfinite observations. Exact input equality determines empirical constancy.
    """
    values = np.asarray(values, dtype=float)
    n = len(values)
    if n == 0:
        return np.nan, np.nan, 0, "insufficient_units"
    if not np.isfinite(values).all():
        return np.nan, np.nan, n, "numerical_failure"
    if np.all(values == values[0]):
        return (
            float(values[0]),
            0.0 if n > 1 else np.nan,
            n,
            "degenerate_distribution" if n > 1 else "insufficient_units",
        )
    origin = float(values[0])
    with np.errstate(over="ignore", invalid="ignore"):
        centered = values - origin
    if not np.isfinite(centered).all():
        magnitude = float(np.max(np.abs(values)))
        centered = values / magnitude
        origin = 0.0
    else:
        magnitude = float(np.max(np.abs(centered)))
        centered = centered / magnitude
    mean = origin + float(centered.mean()) * magnitude
    sd = float(np.std(centered, ddof=1)) * magnitude
    if not np.isfinite(mean) or not np.isfinite(sd) or sd <= 0:
        return mean, np.nan, n, "numerical_failure"
    return mean, sd, n, "ok"


def sample_mean_ci(values, ci_level=0.95):
    """Stable t interval for a unit mean; constant/undefined uncertainty is absent."""
    level = _check_level("ci_level", ci_level)
    mean, sd, n, status = stable_sample_summary(values)
    if status != "ok":
        return mean, np.nan, np.nan
    with np.errstate(over="ignore", invalid="ignore"):
        half = stats.t.isf((1.0 - level) / 2, n - 1) * (sd / np.sqrt(n))
        low, high = mean - half, mean + half
    if not np.isfinite(low) or not np.isfinite(high):
        return mean, np.nan, np.nan
    return mean, float(low), float(high)


def welch_samples(group1, group2):
    """Stable raw-observation Welch result: t, df, p, SE, availability status.

    Direction is group 1 minus group 2. Raw vectors are required to detect exact
    empirical constants before floating-point variance arithmetic introduces noise.
    """
    first, second = (np.asarray(x, dtype=float) for x in (group1, group2))
    if min(len(first), len(second)) < 2:
        return np.nan, np.nan, np.nan, np.nan, "insufficient_units"
    if not np.isfinite(first).all() or not np.isfinite(second).all():
        return np.nan, np.nan, np.nan, np.nan, "numerical_failure"
    constants = [bool(np.all(x == x[0])) for x in (first, second)]
    if all(constants):
        return np.nan, np.nan, np.nan, np.nan, "degenerate_distribution"
    outer_scale = 1.0
    with np.errstate(over="ignore", invalid="ignore"):
        centered = [first - second[0], second - second[0]]
    if not all(np.isfinite(x).all() for x in centered):
        outer_scale = max(float(np.max(np.abs(x))) for x in (first, second))
        centered = [x / outer_scale - second[0] / outer_scale for x in (first, second)]
    magnitude = max(float(np.max(np.abs(x))) for x in centered)
    moments = []
    for values, constant in zip(centered, constants):
        scaled = values / magnitude
        deviations = scaled - scaled[0]
        mean = float(scaled[0] + deviations.mean())
        variance = 0.0 if constant else float(np.var(deviations, ddof=1))
        if not np.isfinite(variance) or (not constant and variance <= 0):
            return np.nan, np.nan, np.nan, np.nan, "numerical_failure"
        moments.append((mean, variance, len(values)))
    t, df, p, scaled_se = welch_t(*moments[0], *moments[1])
    with np.errstate(over="ignore", under="ignore", invalid="ignore"):
        se = (scaled_se * magnitude) * outer_scale
    if not all(np.isfinite(x) for x in (t, df, p, se)) or se <= 0:
        return np.nan, np.nan, np.nan, np.nan, "numerical_failure"
    return float(t), float(df), float(p), float(se), "ok"
