"""MDE is a nonnegative normal planning approximation, not attained test power."""

from statistics import NormalDist

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.stats import primitives


@pytest.mark.parametrize("power", [0.01, 0.49])
@pytest.mark.parametrize(
    "method,args",
    [
        (primitives.mde_from_se, (1,)),
        (primitives.mde_mean, (1, 10, 10)),
        (primitives.mde_proportion, (0.2, 10, 10)),
    ],
)
def test_unsupported_low_planning_power_is_rejected(method, args, power):
    with pytest.raises(InvalidParameterError, match="power"):
        method(*args, power=power)


def _small_mean_stream():
    rows = []
    for arm in ["control", "treatment"]:
        for i, value in enumerate(10 + np.arange(-2, 3) / np.sqrt(2.5)):
            rows.append((arm + str(i), "value", pd.Timestamp("2026-01-01"), arm, value))
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "amount"]),
        {"path_cols": ["user_id"], "segment_cols": ["arm"]},
    )


def test_small_welch_mde_names_approximation_and_requires_power_verification():
    frame = _small_mean_stream().get_ab_test(
        ("arm", "treatment", "control"),
        [
            {
                "name": "value",
                "metric": "column_sum",
                "metric_args": {"column": "amount"},
            }
        ],
        test="welch_t_test",
    )
    row = frame.iloc[0]
    expected = (NormalDist().inv_cdf(0.975) + NormalDist().inv_cdf(0.8)) * np.sqrt(
        2 / 5
    )
    assert row.mde == pytest.approx(expected)
    assert "normal-approximation MDE" in row.reading
    assert "verify power for the chosen test" in row.reading
    assert "the design detects" not in row.reading
    assert "effects outside it are unlikely" not in row.reading


def test_public_low_planning_power_is_rejected():
    with pytest.raises(InvalidParameterError, match="power"):
        _small_mean_stream().get_ab_test(
            ("arm", "treatment", "control"),
            [{"metric": "column_sum", "metric_args": {"column": "amount"}}],
            power=0.01,
        )
