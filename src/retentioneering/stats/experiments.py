"""Estimation and inference over unit contribution matrices; no Eventstream or UI."""

import secrets

import numpy as np
import pandas as pd
from scipy import stats


def _integer(value, name, lower):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, np.integer))
        or value < lower
    ):
        raise ValueError(f"{name} must be an integer >= {lower}")
    return int(value)


def _bootstrap_config(config):
    if config is None:
        return None
    if not isinstance(config, dict):
        raise TypeError("bootstrap must be a dict or None")
    allowed = {
        "method",
        "n_resamples",
        "confidence_level",
        "random_state",
        "batch_size",
    }
    if set(config) - allowed:
        raise ValueError(f"Unknown bootstrap keys: {sorted(set(config) - allowed)}")
    result = {
        "method": "percentile",
        "n_resamples": 9999,
        "confidence_level": 0.95,
        "random_state": None,
        "batch_size": 128,
        **config,
    }
    if result["method"] != "percentile":
        raise ValueError("Preview supports only percentile bootstrap")
    result["n_resamples"] = _integer(result["n_resamples"], "n_resamples", 2)
    result["batch_size"] = _integer(result["batch_size"], "batch_size", 1)
    if (
        isinstance(result["confidence_level"], bool)
        or not 0 < result["confidence_level"] < 1
    ):
        raise ValueError("confidence_level must be strictly between 0 and 1")
    if result["random_state"] is None:
        result["random_state"] = secrets.randbits(32)
    result["random_state"] = _integer(result["random_state"], "random_state", 0)
    return result


def _ratio(values):
    a, b = values[..., 0::2], values[..., 1::2]
    return np.divide(a, b, out=np.full(a.shape, np.nan), where=b > 0)


def _draw_sums(values, config, rng):
    n, width = values.shape
    result = np.empty((config["n_resamples"], width), dtype=float)
    for start in range(0, len(result), config["batch_size"]):
        size = min(config["batch_size"], len(result) - start)
        weights = rng.multinomial(n, np.full(n, 1 / n), size=size)
        result[start : start + size] = weights @ values
    return result


def _constant_statistic(values):
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


def _interval(draws, point, n_units, config, *, relative=False, degenerate=False):
    invalid = int((~np.isfinite(draws)).sum()) if config else 0
    if not np.isfinite(point):
        return (
            np.nan,
            np.nan,
            "undefined_baseline" if relative else "undefined_denominator",
            invalid,
        )
    if config is None:
        return np.nan, np.nan, "not_requested", 0
    if n_units < 2:
        return np.nan, np.nan, "insufficient_units", invalid
    if invalid:
        return np.nan, np.nan, "unstable_denominator", invalid
    if degenerate or np.ptp(draws) == 0:
        return np.nan, np.nan, "degenerate_distribution", 0
    alpha = (1 - config["confidence_level"]) / 2
    lo, hi = np.quantile(draws, [alpha, 1 - alpha], method="linear")
    return float(lo), float(hi), "ok", 0


def _summary(specs, c, t, dc, dt, nc, nt, config):
    cp, tp = _ratio(c.sum(axis=0)), _ratio(t.sum(axis=0))
    effects = tp - cp
    relative = np.divide(tp, cp, out=np.full(tp.shape, np.nan), where=cp > 0) - 1
    if config:
        cd, td = _ratio(dc), _ratio(dt)
        dd = td - cd
        rd = np.divide(td, cd, out=np.full(td.shape, np.nan), where=cd > 0) - 1
    else:
        cd = td = dd = rd = np.empty((0, len(specs)))
    rows = []
    for j, spec in enumerate(specs):
        constant_c = _constant_statistic(c[:, 2 * j : 2 * j + 2])
        constant_t = _constant_statistic(t[:, 2 * j : 2 * j + 2])
        row = dict(spec)
        row.update(
            control_numerator=float(c[:, 2 * j].sum()),
            control_denominator=float(c[:, 2 * j + 1].sum()),
            treatment_numerator=float(t[:, 2 * j].sum()),
            treatment_denominator=float(t[:, 2 * j + 1].sum()),
            control_units=nc,
            treatment_units=nt,
        )
        for arm, values in (("control", c), ("treatment", t)):
            row[f"{arm}_nonzero_units"] = int(np.count_nonzero(values[:, 2 * j]))
            row[f"{arm}_eligible_units"] = int(
                np.count_nonzero(values[:, 2 * j + 1] > 0)
            )
            binary = spec.get("outcome_kind") == "binary"
            row[f"{arm}_successes"] = int(values[:, 2 * j].sum()) if binary else np.nan
            row[f"{arm}_failures"] = (
                int(len(values) - values[:, 2 * j].sum()) if binary else np.nan
            )
        for label, point, draws, n in (
            ("control", cp[j], cd[:, j], nc),
            ("treatment", tp[j], td[:, j], nt),
            ("absolute_effect", effects[j], dd[:, j], min(nc, nt)),
            ("relative_effect", relative[j], rd[:, j], min(nc, nt)),
        ):
            lo, hi, status, invalid = _interval(
                draws,
                point,
                n,
                config,
                relative=label == "relative_effect",
                degenerate=constant_c
                if label == "control"
                else constant_t
                if label == "treatment"
                else constant_c and constant_t,
            )
            if (
                config
                and label in {"absolute_effect", "relative_effect"}
                and status == "ok"
                and (
                    constant_c
                    or constant_t
                    or np.ptp(cd[:, j]) == 0
                    or np.ptp(td[:, j]) == 0
                )
            ):
                lo = hi = np.nan
                status = "degenerate_component"
            row.update(
                {
                    label: float(point),
                    f"{label}_ci_low": lo,
                    f"{label}_ci_high": hi,
                    f"{label}_status": status,
                    f"{label}_invalid_resamples": invalid,
                    f"{label}_ci_method": "bootstrap_percentile" if config else None,
                    f"{label}_ci_confidence_level": config["confidence_level"]
                    if config
                    else None,
                }
            )
        row.update(
            p_value=np.nan,
            p_value_adjusted=np.nan,
            test_method=None,
            test_status="not_requested",
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _binary_interval_config(config):
    if config is None:
        return None
    if not isinstance(config, dict) or set(config) - {"method", "confidence_level"}:
        raise ValueError("binary_intervals accepts method and confidence_level")
    if config.get("method") != "newcombe":
        raise ValueError("binary_intervals requires the explicit method='newcombe'")
    level = config.get("confidence_level", 0.95)
    if (
        isinstance(level, bool)
        or not isinstance(level, (int, float))
        or not 0 < level < 1
    ):
        raise ValueError("binary_intervals confidence_level must be in (0,1)")
    return {"method": "newcombe", "confidence_level": float(level)}


def _wilson(successes, n, confidence_level):
    """Wilson score interval without continuity correction, including boundaries."""
    z = stats.norm.ppf((1 + confidence_level) / 2)
    p = successes / n
    divisor = 1 + z * z / n
    center = (p + z * z / (2 * n)) / divisor
    radius = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / divisor
    # Score inversion includes the exact parameter-space endpoint when all
    # observations agree. Cancellation can otherwise produce e.g. 8e-19 at
    # zero successes, or 1 - 2e-16 at n successes, excluding the true boundary.
    lower = 0.0 if successes == 0 else max(0.0, float(center - radius))
    upper = 1.0 if successes == n else min(1.0, float(center + radius))
    return lower, upper


def _binary_intervals(summary, specs, c, t, config):
    if config is None:
        return
    level = config["confidence_level"]
    for j, spec in enumerate(specs):
        if spec.get("outcome_kind") != "binary":
            continue
        for values in (c, t):
            if (
                not np.isin(values[:, 2 * j], [0.0, 1.0]).all()
                or not (values[:, 2 * j + 1] == 1).all()
            ):
                raise ValueError(
                    "Binary intervals require one Bernoulli outcome per independent assigned unit"
                )
        cp, tp = c[:, 2 * j].mean(), t[:, 2 * j].mean()
        cl, cu = _wilson(c[:, 2 * j].sum(), len(c), level)
        tl, tu = _wilson(t[:, 2 * j].sum(), len(t), level)
        delta = tp - cp
        lower = max(-1.0, delta - np.hypot(tp - tl, cu - cp))
        upper = min(1.0, delta + np.hypot(tu - tp, cp - cl))
        for label, lo, hi, method in (
            ("control", cl, cu, "wilson"),
            ("treatment", tl, tu, "wilson"),
            ("absolute_effect", lower, upper, "newcombe"),
        ):
            summary.loc[j, f"{label}_ci_low"] = float(lo)
            summary.loc[j, f"{label}_ci_high"] = float(hi)
            summary.loc[j, f"{label}_status"] = "ok"
            summary.loc[j, f"{label}_ci_method"] = method
            summary.loc[j, f"{label}_ci_confidence_level"] = level
            # There are no Monte Carlo draws in this selected procedure.
            summary.loc[j, f"{label}_invalid_resamples"] = 0


def adjust_pvalues(pvalues):
    """Holm correction; unavailable tests still count toward the declared family."""
    p = np.asarray(pvalues, dtype=float)
    if np.isinf(p).any() or ((p[np.isfinite(p)] < 0) | (p[np.isfinite(p)] > 1)).any():
        raise ValueError("p-values must be in [0,1] or missing")
    out = np.full(p.shape, np.nan)
    ix = np.flatnonzero(np.isfinite(p))
    ix = ix[np.argsort(p[ix], kind="stable")]
    if len(ix):
        out[ix] = np.minimum(
            1.0, np.maximum.accumulate(p[ix] * (len(p) - np.arange(len(ix))))
        )
    return out


def _welch(control, treatment):
    """Welch over stable moments; input constancy precedes variance arithmetic."""
    constant_c = bool(np.all(control == control[0]))
    constant_t = bool(np.all(treatment == treatment[0]))
    if constant_c and constant_t:
        return np.nan, "degenerate_distribution"
    # Remove a shared offset before scaling to retain small, representable
    # differences around a large baseline. Subtraction can itself overflow for
    # opposite extreme values; only then scale before centering instead.
    with np.errstate(over="ignore", invalid="ignore"):
        centered = [control - control[0], treatment - control[0]]
    if not all(np.isfinite(values).all() for values in centered):
        magnitude = max(
            float(np.max(np.abs(control))), float(np.max(np.abs(treatment)))
        )
        centered = [
            control / magnitude - control[0] / magnitude,
            treatment / magnitude - control[0] / magnitude,
        ]
    magnitude = max(float(np.max(np.abs(values))) for values in centered)
    scaled = [values / magnitude for values in centered]
    moments = []
    for values, constant in zip(scaled, (constant_c, constant_t)):
        # Center each arm before estimating its variance. This also avoids
        # SciPy's precision-loss warnings for a valid single constant arm.
        centered = values - values[0]
        mean_centered = float(centered.mean())
        mean = float(values[0] + mean_centered)
        variance = 0.0 if constant else float(np.var(centered, ddof=1))
        if not np.isfinite(variance) or (not constant and variance <= 0):
            return np.nan, "numerical_failure"
        moments.append((mean, np.sqrt(variance), len(values)))
    cm, cs, nc = moments[0]
    tm, ts, nt = moments[1]
    # Normalize spread as well: the Welch-Satterthwaite calculation squares
    # variances, which can underflow even after the observations were scaled.
    spread = max(cs, ts)
    with np.errstate(over="ignore", under="ignore", divide="ignore", invalid="ignore"):
        result = stats.ttest_ind_from_stats(
            (tm - cm) / spread, ts / spread, nt, 0.0, cs / spread, nc, equal_var=False
        )
    if not np.isfinite(result.statistic) or not np.isfinite(result.pvalue):
        return np.nan, "numerical_failure"
    return float(result.pvalue), "ok"


def _tests(summary, specs, c, t, config):
    if config is None:
        return
    if (
        not isinstance(config, dict)
        or set(config) != {"methods", "adjustment"}
        or config["adjustment"] != "holm"
    ):
        raise ValueError(
            "Preview tests require an explicit methods mapping and adjustment='holm'"
        )
    methods = config["methods"]
    if not isinstance(methods, dict) or not methods:
        raise ValueError("Declare a nonempty test family before reading results")
    lookup = {s["metric"]: j for j, s in enumerate(specs)}
    rows = []
    for name, method in methods.items():
        if method not in {"welch", "fisher_exact"}:
            raise ValueError(f"Unknown test method: {method}")
        if name not in lookup:
            raise ValueError(f"Unknown test metric: {name}")
        j = lookup[name]
        if (
            specs[j]["aggregation"] != "mean"
            and specs[j].get("outcome_kind") != "binary"
        ):
            raise ValueError(
                "Tests support user-level mean outcomes, not ratios or conditional funnels"
            )
        rows.append(j)
        summary.loc[j, "test_method"] = method + "_two_sided"
        if method == "fisher_exact":
            if specs[j].get("outcome_kind") != "binary":
                raise ValueError("Fisher requires an explicitly binary outcome")
            cs, ts = int(c[:, 2 * j].sum()), int(t[:, 2 * j].sum())
            table = [[ts, len(t) - ts], [cs, len(c) - cs]]
            summary.loc[j, "p_value"] = float(
                stats.fisher_exact(table, alternative="two-sided").pvalue
            )
            summary.loc[j, "test_status"] = "ok"
        elif min(len(c), len(t)) < 2:
            summary.loc[j, "test_status"] = "insufficient_units"
        else:
            pvalue, status = _welch(c[:, 2 * j], t[:, 2 * j])
            summary.loc[j, "p_value"] = pvalue
            summary.loc[j, "test_status"] = status
    summary.loc[rows, "p_value_adjusted"] = adjust_pvalues(
        summary.loc[rows, "p_value"].to_numpy()
    )
