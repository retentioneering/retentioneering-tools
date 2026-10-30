"""Tiny-arm exact defaults with independently calculated endpoint expectations."""

import math

import pandas as pd
import pytest

from retentioneering import Eventstream


def tiny():
    return Eventstream(
        pd.DataFrame(
            [
                ("c", "A", "2026-01-01"),
                ("t1", "A", "2026-01-01"),
                ("t1", "B", "2026-01-02"),
                ("t2", "A", "2026-01-01"),
                ("t2", "B", "2026-01-02"),
            ],
            columns=["user_id", "event", "timestamp"],
        )
    )


BASIS = {
    "rate": "clopper_pearson",
    "delta": "clopper_pearson_bonferroni",
    "p_value": "fisher_exact_two_sided",
}
DIFF = (["t1", "t2"], ["c"])


def test_funnel_tiny_arms_exact_and_constant_first_stage():
    out = tiny().funnel_data(["A", "B"], diff=DIFF, ci_level=0.95)
    assert out["ci_basis"] == BASIS
    row = out["steps"][1]
    assert row["delta_conversion_rate_p_value"] == pytest.approx(1 / 3)
    assert row["funnel1_conversion_rate_ci_low"] == pytest.approx(math.sqrt(0.025))
    assert row["funnel2_conversion_rate_ci_high"] == pytest.approx(0.975)
    assert row["delta_conversion_rate_ci_low"] == pytest.approx(
        math.sqrt(0.0125) - 0.9875
    )
    assert row["delta_conversion_rate_ci_high"] == 1
    assert out["steps"][0]["delta_conversion_rate_p_value"] == 1
    assert row["step_conditional"]
    assert "delta_step_conversion_rate_p_value" not in row
    assert out["population_basis"] == "observed_paths"


def test_conversion_exact_methods_metadata_survive_copy_and_reset():
    out = tiny().get_conversion_rate("path_start", "B", diff=DIFF)
    row = out.iloc[0]
    assert row.p_value == pytest.approx(1 / 3)
    assert row.group1_conversion_rate_ci_low == pytest.approx(math.sqrt(0.025))
    assert row.group2_conversion_rate_ci_high == pytest.approx(0.975)
    assert row.delta_ci_low == pytest.approx(math.sqrt(0.0125) - 0.9875)
    assert out.attrs["ci_basis"] == BASIS
    assert out.attrs["ci_level"] == 0.95
    assert out.attrs["population_basis"] == "observed_paths"
    assert out.attrs["independence_assumption"] == "independent_observed_paths"
    assert out.copy().reset_index(drop=True).attrs == out.attrs
    assert tiny().get_conversion_rate("A", "B", diff=DIFF).p_value.isna().all()


def test_single_conversion_exact_metadata():
    out = tiny().get_conversion_rate("A", "B", ci_level=0.95)
    assert out.attrs["ci_basis"] == {"rate": "clopper_pearson"}
    assert out.copy().reset_index(drop=True).attrs == out.attrs
