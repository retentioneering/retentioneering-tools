"""Representable tail probabilities must not be rounded to zero."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.stats import primitives


def _stream():
    rows = []
    for arm, successes in [("A", 5), ("B", 6)]:
        for i in range(10):
            rows.append((f"{arm}{i}", "open", pd.Timestamp("2026-01-01"), arm))
            if i < successes:
                rows.append((f"{arm}{i}", "buy", pd.Timestamp("2026-01-02"), arm))
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"]),
        {"segment_cols": ["arm"]},
    )


def test_highest_representable_confidence_matches_independent_tail_oracle():
    level = np.nextafter(1.0, 0.0)
    np.testing.assert_allclose(
        primitives.wilson_ci(5, 10, level),
        [0.03281769121412237, 0.9671823087858776],
        rtol=1e-13,
    )
    assert np.isfinite(primitives.mde_from_se(1, 1 - level)).all()
    assert np.isfinite(primitives.sample_mean_ci(np.arange(10.0), level)).all()
    assert np.isfinite(primitives.exact_binomial_ci(5, 10, level)).all()


@pytest.mark.parametrize("level", [1 - 1e-13, np.nextafter(1.0, 0.0)])
def test_public_scorecard_retains_small_positive_alpha(level):
    out = _stream().ab_test_data(
        ("arm", "A", "B"),
        [{"metric": "has_event", "metric_args": {"event": "buy"}}],
        ci_level=level,
    )
    assert out.mde_alpha.iloc[0] == 1 - level
    assert np.isfinite(
        out[
            ["group1_ci_low", "group1_ci_high", "delta_ci_low", "delta_ci_high"]
        ].to_numpy()
    ).all()


def test_unrepresentable_bonferroni_level_is_rejected_explicitly():
    metrics = [
        {"name": str(i), "metric": "has_event", "metric_args": {"event": "buy"}}
        for i in range(3)
    ]
    with pytest.raises(InvalidParameterError, match="floating-point precision"):
        _stream().ab_test_data(
            ("arm", "A", "B"),
            metrics,
            ci_level=np.nextafter(1.0, 0.0),
            correction="bonferroni",
        )


@pytest.mark.parametrize("power", [0.01, 0.49])
def test_mde_rejects_low_power_outside_planning_approximation(power):
    with pytest.raises(InvalidParameterError, match="power"):
        primitives.mde_from_se(0.1, power=power)
    with pytest.raises(InvalidParameterError, match="power"):
        _stream().ab_test_data(
            ("arm", "A", "B"),
            [{"metric": "has_event", "metric_args": {"event": "buy"}}],
            power=power,
        )
