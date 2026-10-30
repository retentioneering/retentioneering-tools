"""Exact/default scorecards must not leak unrequested asymptotic diagnostics."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream


def _stream(specs):
    rows = []
    for stratum, treatment_n, treatment_k, control_n, control_k in specs:
        for arm, n, k in [
            ("treatment", treatment_n, treatment_k),
            ("control", control_n, control_k),
        ]:
            for i in range(n):
                uid = f"{stratum}_{arm}_{i}"
                rows.append((uid, "open", pd.Timestamp("2026-01-02"), arm, stratum))
                if i < k:
                    rows.append((uid, "buy", pd.Timestamp("2026-01-03"), arm, stratum))
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "stratum"]),
        {"segment_cols": ["arm", "stratum"]},
    )


def _read(stream, method="auto", metric="has_event"):
    return stream.ab_test_data(
        ("arm", "treatment", "control"),
        [{"metric": metric, "metric_args": {"event": "buy"}}],
        strata_col="stratum",
        strata_pre_exposure=True,
        test=method,
    )


@pytest.mark.parametrize(
    "method,metric",
    [("auto", "has_event"), ("descriptive", "has_event"), ("auto", "event_count")],
)
def test_default_or_descriptive_does_not_request_secondary_tests(method, metric):
    out = _read(_stream([("a", 1, 1, 500, 100), ("b", 1, 0, 500, 100)]), method, metric)
    assert (
        out[["balance_p_value", "heterogeneity_p_value", "stratum_vs_rest_p_value"]]
        .isna()
        .all()
        .all()
    )
    assert set(out.strata_diagnostics_basis) == {"not_requested"}
    assert out.group1_share.tolist() == [0.5, 0.5]
    assert out.group2_share.tolist() == [0.5, 0.5]
    if method == "auto" and metric == "has_event":
        assert out.iloc[0].p_value == pytest.approx(101 / 501)


def test_explicit_secondary_normal_test_requires_support_on_both_sides():
    # The large stratum alone passes primary z support, but its rest does not.
    out = _read(_stream([("a", 100, 40, 500, 100), ("b", 1, 0, 500, 100)]), "z_test")
    assert np.isfinite(out.iloc[0].p_value)
    assert out.stratum_vs_rest_p_value.isna().all()
    assert set(out.strata_diagnostics_basis) == {"asymptotic_exploratory"}


def test_explicit_supported_diagnostics_remain_labelled_approximations():
    out = _read(_stream([("a", 100, 40, 100, 20), ("b", 100, 20, 100, 20)]), "z_test")
    assert (
        np.isfinite(
            out[["balance_p_value", "heterogeneity_p_value", "stratum_vs_rest_p_value"]]
        )
        .all()
        .all()
    )
    assert set(out.strata_diagnostics_basis) == {"asymptotic_exploratory"}


def test_balance_requires_non_sparse_expected_cells():
    out = _read(_stream([("a", 100, 40, 100, 20), ("b", 1, 0, 1, 1)]), "z_test")
    assert out.balance_p_value.isna().all()
