"""Estimation and inference over unit contribution matrices; no Eventstream or UI."""

import secrets

import numpy as np
import pandas as pd

from retentioneering.stats import primitives as _core
from retentioneering.stats.resampling import (
    checked_difference,
    checked_ratio,
    checked_sum,
    draw_sums,
    percentile_interval,
)


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
    return checked_ratio(a, b)


def _draw_sums(values, config, rng):
    return draw_sums(values, config["n_resamples"], config["batch_size"], rng)


def _constant_statistic(values):
    return _core.constant_statistic(values)


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
    return percentile_interval(draws, config["confidence_level"], degenerate=degenerate)


def _summary(specs, c, t, dc, dt, nc, nt, config):
    csum, tsum = checked_sum(c, axis=0), checked_sum(t, axis=0)
    cp, tp = _ratio(csum), _ratio(tsum)
    effects = checked_difference(tp, cp)
    relative = checked_ratio(tp, cp) - 1
    if config:
        cd, td = _ratio(dc), _ratio(dt)
        dd = checked_difference(td, cd)
        rd = checked_ratio(td, cd) - 1
    else:
        cd = td = dd = rd = np.empty((0, len(specs)))
    rows = []
    for j, spec in enumerate(specs):
        constant_c = _constant_statistic(c[:, 2 * j : 2 * j + 2])
        constant_t = _constant_statistic(t[:, 2 * j : 2 * j + 2])
        row = dict(spec)
        row.update(
            control_numerator=float(csum[2 * j]),
            control_denominator=float(csum[2 * j + 1]),
            treatment_numerator=float(tsum[2 * j]),
            treatment_denominator=float(tsum[2 * j + 1]),
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
    if config.get("method") not in {"exact", "newcombe"}:
        raise ValueError(
            "binary_intervals requires method='exact' or method='newcombe'"
        )
    level = config.get("confidence_level", 0.95)
    if (
        isinstance(level, bool)
        or not isinstance(level, (int, float))
        or not 0 < level < 1
    ):
        raise ValueError("binary_intervals confidence_level must be in (0,1)")
    return {"method": config["method"], "confidence_level": float(level)}


def _wilson(successes, n, confidence_level):
    return _core.wilson_ci(successes, n, confidence_level)


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
        exact = config["method"] == "exact"
        arm_ci = _core.exact_binomial_ci if exact else _wilson
        difference_ci = _core.exact_difference_ci if exact else _core.newcombe_ci
        arm_method = "clopper_pearson" if exact else "wilson_approximate"
        difference_method = (
            "clopper_pearson_bonferroni" if exact else "newcombe_approximate"
        )
        cl, cu = arm_ci(c[:, 2 * j].sum(), len(c), level)
        tl, tu = arm_ci(t[:, 2 * j].sum(), len(t), level)
        lower, upper = difference_ci(
            t[:, 2 * j].sum(), len(t), c[:, 2 * j].sum(), len(c), level
        )
        for label, lo, hi, method in (
            ("control", cl, cu, arm_method),
            ("treatment", tl, tu, arm_method),
            ("absolute_effect", lower, upper, difference_method),
        ):
            summary.loc[j, f"{label}_ci_low"] = float(lo)
            summary.loc[j, f"{label}_ci_high"] = float(hi)
            summary.loc[j, f"{label}_status"] = (
                "ok" if np.isfinite(lo) and np.isfinite(hi) else "numerical_failure"
            )
            summary.loc[j, f"{label}_ci_method"] = method
            summary.loc[j, f"{label}_ci_confidence_level"] = level
            # There are no Monte Carlo draws in this selected procedure.
            summary.loc[j, f"{label}_invalid_resamples"] = 0


def adjust_pvalues(pvalues):
    """Holm correction over the full declared family, including unavailable tests."""
    return _core.holm(pvalues)


def _welch(control, treatment):
    """Adapter retaining treatment-minus-control orientation."""
    _, _, pvalue, _, status = _core.welch_samples(treatment, control)
    return pvalue, status


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
        if method == "welch" and specs[j].get("outcome_kind") == "binary":
            raise ValueError(
                "Known binary outcomes require Fisher inference; use fisher_exact"
            )
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
            summary.loc[j, "p_value"] = float(
                _core.fisher_exact_p(ts, len(t), cs, len(c))
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
