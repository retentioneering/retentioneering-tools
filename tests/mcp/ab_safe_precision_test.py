"""Machine payloads preserve inference boundaries; only displays may round."""

import json
import math
from fractions import Fraction

import pandas as pd
import pytest
from scipy.stats import t

from retentioneering import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession
from retentioneering.tools.ab_test import ABTest


@pytest.mark.parametrize("value", [1e-12, 0.0499999998, 0.0500000002, 1e-9, -1e-9])
def test_json_number_preserves_boundary_and_tiny_values(value):
    assert tools._json_number(value) == value


def small_values():
    rows = []
    for arm in ["c", "t"]:
        for i in range(40):
            amount = (i % 4 + 1) * 1e-10 + (1e-9 if arm == "t" else 0)
            rows.append([f"{arm}{i}", "open", pd.Timestamp("2026-01-01"), arm, amount])
            if arm == "t":
                rows.append([f"{arm}{i}", "buy", pd.Timestamp("2026-01-02"), arm, 0.0])
    return Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "amount"]),
        {"segment_cols": ["arm"]},
    )


def test_public_ab_conversion_and_srm_preserve_positive_tiny_p_values():
    s = small_values()
    session = ReportSession(s)
    metric = [
        {"name": "converted", "metric": "has_event", "metric_args": {"event": "buy"}}
    ]
    direct = s.get_ab_test(("arm", "t", "c"), metric).iloc[0]
    row = tools.get_ab_test(session, ["arm", "t", "c"], metric)["metrics"][0]
    expected = float(Fraction(2, math.comb(80, 40)))
    assert 0 < row["p_value"] < 1e-9
    assert row["p_value"] == direct["p_value"]
    assert row["p_value"] == pytest.approx(expected, rel=1e-12, abs=0)
    conv = s.get_conversion_rate(
        "path_start", "buy", diff=("arm", "t", "c"), ci_level=0.95
    ).iloc[0]
    reply = tools.get_conversion_rate(
        session, "path_start", "buy", diff=["arm", "t", "c"], ci_level=0.95
    )["rows"][0]
    assert reply["p_value"] == conv["p_value"] > 0
    for key in [
        "delta",
        "delta_ci_low",
        "delta_ci_high",
        "group1_conversion_rate_ci_low",
    ]:
        assert reply[key] == conv[key]
    assignment = pd.Series(
        ["c"] * 80, index=[f"{arm}{i}" for arm in ["c", "t"] for i in range(40)]
    )
    srm = tools.get_sample_ratio(
        ReportSession(s, assignment=assignment), expected_shares={"c": 0.5, "t": 0.5}
    )
    assert srm["p_value"] == float(Fraction(2, 2**80))
    json.dumps([row, reply, srm], allow_nan=False)


def test_ab_add_and_contrast_preserve_tiny_effect_and_interval_endpoints():
    s = small_values()
    session = ReportSession(s)
    metrics = [
        {"name": "amount", "metric": "column_sum", "metric_args": {"column": "amount"}}
    ]
    direct = s.get_ab_test(("arm", "t", "c"), metrics, test="welch_t_test").iloc[0]
    reply = tools.get_ab_test(session, ["arm", "t", "c"], metrics, test="welch_t_test")[
        "metrics"
    ][0]
    added = tools.add_ab_test(
        session, "Tiny", ["arm", "t", "c"], metrics, test="welch_t_test"
    )["metrics"][0]
    assert 0 < direct["delta_ci_low"] < 1e-6
    for key in ["delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted", "mde"]:
        assert reply[key] == direct[key]
        assert added[key] == direct[key]
    contrast = {"change": (["2026-01-01"], ["2026-01-02"])}
    core = (
        ABTest.from_stream(
            s,
            ("arm", "t", "c"),
            metrics,
            period="D",
            observation_end="2026-01-03",
            n_bootstrap=1000,
        )
        .contrast_summary(contrast)
        .iloc[0]
    )
    result = tools.get_ab_bootstrap(
        session,
        ["arm", "t", "c"],
        metrics,
        period="D",
        contrast=contrast,
        observation_end="2026-01-03",
        n_bootstrap=1000,
    )["rows"][0]
    for key in [
        "delta",
        "delta_ci_low",
        "delta_ci_high",
        "p_value",
        "p_value_adjusted",
        "share_positive",
    ]:
        assert result[key] == core[key]


def test_public_welch_boundary_is_not_rounded_to_alpha():
    # Two equal-variance arms of 40 symmetric -1/+1 observations. Construct
    # a mean shift just below alpha; this checks serialization, not Welch's
    # calibration. The reported Python result is the parity reference.
    shift = float(t.isf(0.0499999998 / 2, 78)) * math.sqrt(2 / 39)
    rows = [
        [
            f"{arm}{i}",
            "open",
            pd.Timestamp("2026-01-01"),
            arm,
            (-1 if i % 2 else 1) + (shift if arm == "t" else 0),
        ]
        for arm in ["c", "t"]
        for i in range(40)
    ]
    s = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "amount"]),
        {"segment_cols": ["arm"]},
    )
    metrics = [
        {"name": "amount", "metric": "column_sum", "metric_args": {"column": "amount"}}
    ]
    direct = s.get_ab_test(("arm", "t", "c"), metrics, test="welch_t_test").iloc[0]
    reply = tools.get_ab_test(
        ReportSession(s), ["arm", "t", "c"], metrics, test="welch_t_test"
    )["metrics"][0]
    assert 0.04999999 < direct["p_value_adjusted"] < 0.05
    assert reply["p_value_adjusted"] == direct["p_value_adjusted"]
    assert reply["verdict"] == direct["verdict"] == "increase"


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_json_number_nonfinite_still_null(value):
    assert tools._json_number(value) is None
