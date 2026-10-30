"""Shared whole-unit weights, weighted sums and percentile availability policy.

The callers own the analysis population and independent unit. Every column of a
unit's contribution matrix receives the same draw weight. Undefined denominator
draws are not silently discarded to manufacture an interval for a new population.
"""

import numpy as np

BOOTSTRAP_ALGORITHM = "whole_unit_counts_v1_baseline_first"
POOLED_BOOTSTRAP_ALGORITHM = "whole_unit_counts_v1_pooled_union"
MAX_BOOTSTRAP_REPLICATES = 1_000_000
MAX_BOOTSTRAP_VALUES = 10_000_000


def check_bootstrap_size(n_resamples, width=1):
    """Reject unsupported requests before allocating replicate arrays.

    These are resource limits, not statistical precision recommendations. Width
    counts stored contribution columns, including numerators and denominators.
    """
    if (
        n_resamples > MAX_BOOTSTRAP_REPLICATES
        or n_resamples * max(1, width) > MAX_BOOTSTRAP_VALUES
    ):
        raise ValueError(
            "Bootstrap resource limit exceeded: at most "
            f"{MAX_BOOTSTRAP_REPLICATES:,} replicates and "
            f"{MAX_BOOTSTRAP_VALUES:,} replicate-by-contribution values; "
            "reduce n_bootstrap/n_resamples or the requested metric family"
        )


def checked_sum(values, axis=None):
    """Sum finite contributions or explicitly reject unsupported numeric range."""
    with np.errstate(over="ignore", invalid="ignore"):
        result = np.asarray(values, dtype=float).sum(axis=axis)
    if not np.isfinite(result).all():
        raise ValueError(
            "Numeric overflow or nonfinite aggregate in unit contributions; "
            "rescale the measurement before analysis"
        )
    return result


def checked_ratio(numerator, denominator):
    """Positive-denominator ratios; undefined denominators remain NaN."""
    top, bottom = np.broadcast_arrays(
        np.asarray(numerator, dtype=float), np.asarray(denominator, dtype=float)
    )
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        result = np.divide(
            top, bottom, out=np.full(top.shape, np.nan), where=bottom > 0
        )
    if np.isinf(result).any():
        raise ValueError("Numeric overflow in a ratio; rescale the measurement")
    return result


def checked_difference(left, right):
    """Subtract estimates while preserving intentionally undefined values."""
    with np.errstate(over="ignore", invalid="ignore"):
        result = np.asarray(left) - np.asarray(right)
    if np.isinf(result).any():
        raise ValueError("Numeric overflow in an effect; rescale the measurement")
    return result


def draw_weights(rng, scheme, size, n):
    if scheme == "poisson":
        return rng.poisson(1.0, size=(size, n)).astype(float)
    if scheme != "multinomial":
        raise ValueError("weights must be multinomial or poisson")
    return rng.multinomial(n, np.full(n, 1 / n), size=size)


def draw_sums(values, n_resamples, batch_size, rng, weights="multinomial"):
    values = np.asarray(values, dtype=float)
    n, width = values.shape
    check_bootstrap_size(n_resamples, width)
    checked_sum(values, axis=0)
    if n > MAX_BOOTSTRAP_VALUES:
        raise ValueError("Bootstrap resource limit exceeded: too many unit weights")
    batch_size = min(batch_size, max(1, MAX_BOOTSTRAP_VALUES // max(1, n)))
    result = np.empty((n_resamples, width), dtype=float)
    if n == 0:
        result.fill(0)
        return result
    for start in range(0, n_resamples, batch_size):
        size = min(batch_size, n_resamples - start)
        with np.errstate(over="ignore", invalid="ignore"):
            sums = draw_weights(rng, weights, size, n) @ values
        if not np.isfinite(sums).all():
            raise ValueError(
                "Numeric overflow in bootstrap weighted sums; "
                "rescale the measurement before analysis"
            )
        result[start : start + size] = sums
    return result


def percentile_interval(draws, confidence_level, *, min_replicates=2, degenerate=False):
    """Return low, high, availability status and undefined-draw count.

    A percentile interval requires every requested draw to be finite. The minimum
    number of draws is explicit: the scorecard requires 100; the strict adapter
    accepts smaller diagnostic runs and reports its low-resample warning.
    """
    draws = np.asarray(draws, dtype=float)
    if draws.ndim != 1:
        raise ValueError("percentile_interval requires one-dimensional draws")
    if isinstance(confidence_level, bool) or not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be strictly between 0 and 1")
    invalid = int((~np.isfinite(draws)).sum())
    if invalid:
        return np.nan, np.nan, "unstable_denominator", invalid
    if len(draws) < min_replicates:
        return np.nan, np.nan, "insufficient_resamples", 0
    if degenerate or np.all(draws == draws[0]):
        return np.nan, np.nan, "degenerate_distribution", 0
    tail = (1 - confidence_level) / 2
    low, high = np.quantile(draws, [tail, 1 - tail], method="linear")
    return float(low), float(high), "ok", 0
