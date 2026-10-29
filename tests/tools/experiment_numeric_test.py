"""Public-API regressions for decimal degeneracy and measurement-scale changes."""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering import Eventstream

OBSERVATION = {
    "start": "2026-01-02",
    "end": "2026-01-05",
    "assignment_time_col": "assigned_at",
    "complete": True,
}


def calculate(control, treatment, *, denominators=None, bootstrap=True, welch=True):
    ids = [f"c{i:03}" for i in range(len(control))] + [
        f"t{i:03}" for i in range(len(treatment))
    ]
    events = pd.DataFrame(
        {
            "user_id": ids,
            "event": "outcome",
            "timestamp": "2026-01-03",
            "amount": list(control) + list(treatment),
        }
    )
    component = {
        "metric": "event_value_sum",
        "metric_args": {"event": "outcome", "value_col": "amount"},
    }
    metric = dict(name="outcome", agg="mean", **component)
    if denominators is not None:
        events["denominator"] = denominators
        metric = {
            "name": "outcome",
            "agg": "ratio_of_sums",
            "numerator": component,
            "denominator": {
                "metric": "event_value_sum",
                "metric_args": {"event": "outcome", "value_col": "denominator"},
            },
        }
    stream = Eventstream(events)
    assignments = pd.DataFrame(
        {
            "user_id": ids,
            "variant": ["c"] * len(control) + ["t"] * len(treatment),
            "assigned_at": "2026-01-01",
        }
    )
    payload = stream.experiment_data(
        assignments=assignments,
        variant_col="variant",
        control="c",
        treatment="t",
        metrics=[metric],
        observation=OBSERVATION,
        bootstrap={"n_resamples": 999, "random_state": 42} if bootstrap else None,
        tests={"methods": {"outcome": "welch"}, "adjustment": "holm"}
        if welch
        else None,
    )
    return payload["summary"][0]


@pytest.mark.parametrize("n", [10, 30, 100])
def test_constant_decimal_means_have_no_fabricated_precision(n):
    row = calculate([0.1] * n, [0.2] * n)
    assert row["absolute_effect"] == pytest.approx(0.1)
    for name in ("control", "treatment", "absolute_effect", "relative_effect"):
        assert row[f"{name}_status"] == "degenerate_distribution"
        assert row[f"{name}_ci_low"] is None
        assert row[f"{name}_ci_high"] is None
    assert row["test_status"] == "degenerate_distribution"
    assert row["p_value"] is row["p_value_adjusted"] is None


def test_one_constant_decimal_component_retains_bootstrap_safety_policy():
    row = calculate([0.1] * 30, np.linspace(0.11, 0.3, 30))
    assert row["control_status"] == "degenerate_distribution"
    assert row["treatment_status"] == "ok"
    assert row["absolute_effect_status"] == "degenerate_component"
    assert row["absolute_effect_ci_low"] is None
    assert row["test_status"] == "ok"  # Welch permits one nonzero-variance arm.
    expected = stats.ttest_ind_from_stats(
        np.linspace(0.11, 0.3, 30).mean(),
        np.linspace(0.11, 0.3, 30).std(ddof=1),
        30,
        0.1,
        0.0,
        30,
        equal_var=False,
    ).pvalue
    assert row["p_value"] == pytest.approx(expected, rel=1e-12)


def test_constant_ratios_with_varying_denominators_and_zero_pairs():
    d = np.tile([0.0, 1.0, 3.0, 7.0], 10)
    row = calculate(0.1 * d, 0.2 * d, denominators=np.r_[d, d], welch=False)
    for name in ("control", "treatment", "absolute_effect", "relative_effect"):
        assert row[f"{name}_status"] == "degenerate_distribution"
        assert row[f"{name}_ci_low"] is None


def test_nonzero_numerator_at_zero_denominator_is_not_constant_ratio():
    # Positive-denominator units all have the same individual rate, but a unit
    # with numerator and no denominator changes the pooled ratio when resampled.
    d = np.r_[np.ones(29), 0.0]
    row = calculate(
        np.r_[np.full(29, 0.1), 0.2],
        np.r_[np.full(29, 0.2), 0.3],
        denominators=np.r_[d, d],
        welch=False,
    )
    assert row["control_status"] == row["treatment_status"] == "ok"
    assert row["absolute_effect_status"] == "ok"


@pytest.mark.parametrize("scale", [1.0, 1e-200, 1e200])
def test_welch_is_invariant_to_common_finite_measurement_scale(scale):
    c, t = np.array([1.0, 2.0, 3.0]), np.array([2.0, 3.0, 4.0])
    row = calculate(c * scale, t * scale, bootstrap=False)
    expected = stats.ttest_ind(t, c, equal_var=False).pvalue
    assert row["test_status"] == "ok"
    assert row["p_value"] == pytest.approx(expected, rel=1e-12)


@pytest.mark.parametrize("scale", [1.0, 1e-200])
def test_genuinely_varying_small_means_are_not_marked_constant(scale):
    c = np.linspace(0.1, 0.2, 30) * scale
    t = np.linspace(0.11, 0.25, 30) * scale
    row = calculate(c, t)
    for name in ("control", "treatment", "absolute_effect"):
        assert row[f"{name}_status"] == "ok"
        assert row[f"{name}_ci_low"] < row[f"{name}_ci_high"]
    assert row["test_status"] == "ok"


def test_genuinely_varying_tiny_ratios_have_scale_relative_degeneracy_checks():
    d = np.tile([1.0, 3.0, 7.0], 10)
    c = np.linspace(1e-200, 2e-200, len(d)) * d
    t = np.linspace(1.1e-200, 2.5e-200, len(d)) * d
    row = calculate(c, t, denominators=np.r_[d, d], welch=False)
    assert row["control_status"] == row["treatment_status"] == "ok"
    assert row["absolute_effect_status"] == "ok"


def test_nonrepresentable_variance_is_unavailable_instead_of_successful():
    # The varying control arm is too small relative to the treatment constant
    # for its variance to survive float64 arithmetic after common scaling.
    row = calculate([-1e-200, 0.0, 1e-200], [1e200] * 3, bootstrap=False)
    assert row["test_status"] == "numerical_failure"
    assert row["p_value"] is row["p_value_adjusted"] is None


def test_welch_preserves_small_differences_around_a_large_shared_offset():
    control = np.array([1.0, 2.0, 3.0])
    treatment = np.array([2.0, 3.0, 4.0])
    row = calculate(control + 1e14, treatment + 1e14, bootstrap=False)
    expected = stats.ttest_ind(treatment, control, equal_var=False).pvalue
    assert row["test_status"] == "ok"
    assert row["p_value"] == pytest.approx(expected, rel=1e-12)
