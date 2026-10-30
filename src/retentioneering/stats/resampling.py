"""Shared whole-unit weights, weighted sums and percentile availability policy.

The callers own the analysis population and independent unit. Every column of a
unit's contribution matrix receives the same draw weight. Undefined denominator
draws are not silently discarded to manufacture an interval for a new population.
"""

import numpy as np

BOOTSTRAP_ALGORITHM = "whole_unit_counts_v1_baseline_first"
POOLED_BOOTSTRAP_ALGORITHM = "whole_unit_counts_v1_pooled_union"


def draw_weights(rng, scheme, size, n):
    if scheme == "poisson":
        return rng.poisson(1.0, size=(size, n)).astype(float)
    if scheme != "multinomial":
        raise ValueError("weights must be multinomial or poisson")
    return rng.multinomial(n, np.full(n, 1 / n), size=size)


def draw_sums(values, n_resamples, batch_size, rng, weights="multinomial"):
    values = np.asarray(values, dtype=float)
    n, width = values.shape
    result = np.empty((n_resamples, width), dtype=float)
    if n == 0:
        result.fill(0)
        return result
    for start in range(0, n_resamples, batch_size):
        size = min(batch_size, n_resamples - start)
        result[start : start + size] = draw_weights(rng, weights, size, n) @ values
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
