"""Safe defaults: exact binary inference and explicit nonbinary approximations."""

from math import comb

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.stats import primitives
from retentioneering.tools.ab_test import ABMetricSpec
from retentioneering.tools.experiment import experiment_data


def fixture(ka=190, na=1000, kb=1, nb=1):
    rows = []
    assigned = []
    for arm, successes, n in [("control", ka, na), ("treatment", kb, nb)]:
        for i in range(n):
            uid = f"{arm}_{i}"
            assigned.append((uid, arm, pd.Timestamp("2026-01-01")))
            rows.append((uid, "open", pd.Timestamp("2026-01-03"), arm))
            if i < successes:
                rows.append((uid, "buy", pd.Timestamp("2026-01-04"), arm))
    stream = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"]),
        {"segment_cols": ["arm"]},
    )
    assignments = pd.DataFrame(assigned, columns=["user_id", "arm", "assigned_at"])
    return stream, assignments


CONVERSION = {
    "name": "conversion",
    "metric": "has_event",
    "metric_args": {"event": "buy"},
}


def test_binary_default_uses_exact_test_and_conservative_interval():
    stream, assignments = fixture()
    row = stream.ab_test_data(
        ("arm", "treatment", "control"),
        [CONVERSION],
        assignment=assignments.set_index("user_id"),
    ).iloc[0]
    assert row.test == "fisher_exact"
    assert row.p_basis == "fisher_exact_two_sided"
    assert row.ci_basis == "clopper_pearson_bonferroni"
    assert row.mde_basis == "normal_approximation"
    assert row.p_value == pytest.approx(191 / 1001)
    assert row.verdict == "inconclusive"
    assert row.delta_ci_low <= 0 <= row.delta_ci_high
    assert np.isnan(row.rel_delta_ci_low)


def test_explicit_z_withholds_tiny_arm_and_labels_approximation():
    stream, assignments = fixture()
    row = stream.ab_test_data(
        ("arm", "treatment", "control"),
        [CONVERSION],
        assignment=assignments.set_index("user_id"),
        test="z_test",
    ).iloc[0]
    assert np.isnan(row.p_value)
    assert row.verdict == "insufficient_data"
    assert row.support_warning == "insufficient_expected_cells"
    assert row.p_basis == "pooled_z_asymptotic"
    assert "approximate" in row.ci_basis


@pytest.mark.parametrize("method", ["welch_t_test", "bootstrap"])
def test_known_binary_welch_rejected(method):
    with pytest.raises(InvalidMetricConfigError, match="binary|0/1"):
        ABMetricSpec.from_user({**CONVERSION, "test": method})


def test_strict_binary_welch_rejected():
    stream, assignments = fixture(0, 2, 9, 100)
    with pytest.raises(ValueError, match="binary|Bernoulli"):
        experiment_data(
            stream,
            assignments=assignments,
            metrics=[{**CONVERSION, "agg": "mean"}],
            variant_col="arm",
            observation={
                "start": "2026-01-02",
                "end": "2026-01-06",
                "assignment_time_col": "assigned_at",
                "complete": True,
            },
            tests={"methods": {"conversion": "welch"}, "adjustment": "holm"},
        )


def test_nonbinary_auto_has_no_implicit_inference_or_bootstrap(monkeypatch):
    from retentioneering.tools.ab_test import ABTest

    stream, _ = fixture(10, 20, 15, 20)

    def forbidden(*args, **kwargs):
        raise AssertionError("descriptive default allocated resampling")

    monkeypatch.setattr(ABTest, "_run_bootstrap", forbidden)
    row = stream.ab_test_data(
        ("arm", "treatment", "control"),
        [{"metric": "event_count", "metric_args": {"event": "buy"}}],
    ).iloc[0]
    assert row.test == "descriptive"
    assert row.verdict == "descriptive"
    assert row.delta == pytest.approx(0.25)
    assert np.isnan(
        row[["p_value", "delta_ci_low", "delta_ci_high", "mde"]].astype(float)
    ).all()
    assert "no inferential method requested" in row.reading


def test_exact_difference_boundary_and_high_confidence():
    # CP simultaneous arm confidence uses alpha/4 in each one-sided tail.
    low, high = primitives.exact_difference_ci(1, 1, 0, 1)
    assert low == pytest.approx(-0.975)
    assert high == 1
    assert np.isfinite(
        primitives.exact_difference_ci(5, 10, 6, 10, np.nextafter(1.0, 0.0))
    ).all()


def test_exact_difference_coverage_by_enumeration():
    na, nb = 2, 3
    intervals = {
        (a, b): primitives.exact_difference_ci(b, nb, a, na)
        for a in range(na + 1)
        for b in range(nb + 1)
    }
    for pa in [0, 0.01, 0.1, 0.5, 0.9, 0.99, 1]:
        for pb in [0, 0.01, 0.1, 0.5, 0.9, 0.99, 1]:
            coverage = sum(
                comb(na, a)
                * pa**a
                * (1 - pa) ** (na - a)
                * comb(nb, b)
                * pb**b
                * (1 - pb) ** (nb - b)
                for (a, b), (low, high) in intervals.items()
                if low <= pb - pa <= high
            )
            assert coverage >= 0.95 - 1e-14


def test_fisher_shared_primitive_handles_constants_and_broadcasts():
    np.testing.assert_allclose(
        primitives.fisher_exact_p(
            [1, 0, 10], [1, 10, 10], [190, 0, 10], [1000, 10, 10]
        ),
        [191 / 1001, 1, 1],
    )
    assert np.isnan(primitives.fisher_exact_p(0, 0, 1, 10))


def test_strict_exact_binary_interval_option():
    stream, assignments = fixture(5, 10, 6, 10)
    result = experiment_data(
        stream,
        assignments=assignments,
        metrics=[{**CONVERSION, "agg": "mean"}],
        variant_col="arm",
        observation={
            "start": "2026-01-02",
            "end": "2026-01-06",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        binary_intervals={"method": "exact"},
    )
    row = result.summary.iloc[0]
    assert row.control_ci_method == "clopper_pearson"
    assert row.absolute_effect_ci_method == "clopper_pearson_bonferroni"
    assert row.absolute_effect_status == "ok"


def test_srm_structural_zero_categories_do_not_inflate_degrees_of_freedom():
    expected = primitives.srm_chi2([68, 32], [0.5, 0.5])
    assert primitives.srm_chi2([68, 32, 0], [0.5, 0.5, 0]) == pytest.approx(expected)
    assert expected[0] == pytest.approx(12.96)
    assert expected[2] == 1
    assert primitives.srm_chi2([1, 99], [0, 1]) == (float("inf"), 0.0, 0)
