"""`get_conversion_rate(ci_level=, diff=)`: intervals and a path-level group comparison."""

import json

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import DiffConfigError, InvalidParameterError
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession
from retentioneering.tools.conversion import DIFF_COLUMNS


def _lumeo_like(n: int = 600, seed: int = 11) -> Eventstream:
    """Users with a constant exp_group; some add to cart, some of those order."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        arm = "treatment" if i % 2 else "control"
        t = pd.Timestamp("2024-01-01") + pd.Timedelta(hours=i)
        events = ["visit", "catalog"]
        if rng.random() < 0.5:
            events.append("add_to_cart")
            if rng.random() < (0.35 if arm == "treatment" else 0.3):
                events.append("order_success")
        for j, e in enumerate(events):
            rows.append((f"u{i}", e, t + pd.Timedelta(minutes=j), arm))
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "exp_group"])
    return Eventstream(df, {"segment_cols": ["exp_group"]})


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return _lumeo_like()


def _counts(stream, arm):
    df = stream.df[stream.df.exp_group == arm]
    carts = set(df.loc[df.event == "add_to_cart", "user_id"])
    orders = set(df.loc[df.event == "order_success", "user_id"])
    return len(carts & orders), len(carts), len(orders) / df.user_id.nunique()


def test_ci_columns(stream) -> None:
    plain = stream.get_conversion_rate("add_to_cart", "order_success")
    result = stream.get_conversion_rate("add_to_cart", "order_success", ci_level=0.9)
    assert list(result.columns) == [
        "start_anchor",
        "end_anchor",
        "paths_with_start",
        "converted",
        "conversion_rate",
        "conversion_rate_ci_low",
        "conversion_rate_ci_high",
        "base_rate",
        "lift",
    ]
    pd.testing.assert_frame_equal(result[plain.columns], plain)
    row = result.iloc[0]
    ref = stats.binomtest(int(row.converted), int(row.paths_with_start)).proportion_ci(
        0.9, method="exact"
    )
    assert row.conversion_rate_ci_low == pytest.approx(ref.low)
    assert row.conversion_rate_ci_high == pytest.approx(ref.high)


def test_ci_nan_without_start(stream) -> None:
    result = stream.get_conversion_rate("order_success", "add_to_cart", ci_level=0.95)
    assert result.iloc[0].paths_with_start > 0
    assert result.iloc[0].converted == 0
    assert result.iloc[0].conversion_rate_ci_low == 0.0
    empty = stream.get_conversion_rate(
        "order_success->.*->visit", "catalog", ci_level=0.95
    ).iloc[0]
    assert empty.paths_with_start == 0
    assert np.isnan(empty.conversion_rate_ci_low) and np.isnan(
        empty.conversion_rate_ci_high
    )


def test_ci_level_validated(stream) -> None:
    for bad in (0, 0.3, 1, 1.5, "0.95", True):
        with pytest.raises(InvalidParameterError):
            stream.get_conversion_rate("add_to_cart", "order_success", ci_level=bad)


def test_diff_matches_hand_counts(stream) -> None:
    result = stream.get_conversion_rate(
        "add_to_cart", "order_success", diff=("exp_group", "treatment", "control")
    )
    assert list(result.columns) == DIFF_COLUMNS
    row = result.iloc[0]
    k1, n1, base1 = _counts(stream, "treatment")
    k2, n2, base2 = _counts(stream, "control")
    assert (row.group1_converted, row.group1_paths_with_start) == (k1, n1)
    assert (row.group2_converted, row.group2_paths_with_start) == (k2, n2)
    assert row.group1_base_rate == pytest.approx(base1)
    assert row.group2_base_rate == pytest.approx(base2)
    assert row.delta == pytest.approx(k1 / n1 - k2 / n2)
    assert row.conditional
    assert np.isnan(row.p_value)  # selected after exposure: descriptive only
    assert row.delta_ci_low < row.delta < row.delta_ci_high
    assert "unassigned_paths" not in row.index  # overlapping groups raise instead
    ref = stats.binomtest(int(k1), int(n1)).proportion_ci(0.95, method="exact")
    assert row.group1_conversion_rate_ci_low == pytest.approx(ref.low)


def test_diff_level_changes_intervals(stream) -> None:
    diff = ("exp_group", "treatment", "control")
    wide = stream.get_conversion_rate(
        "add_to_cart", "order_success", ci_level=0.99, diff=diff
    )
    narrow = stream.get_conversion_rate(
        "add_to_cart", "order_success", ci_level=0.8, diff=diff
    )
    assert wide.iloc[0].delta_ci_low < narrow.iloc[0].delta_ci_low
    assert np.isnan(wide.iloc[0].p_value) and np.isnan(narrow.iloc[0].p_value)


def test_diff_fan_out_and_event_missing_in_one_group() -> None:
    stream = _lumeo_like(200)
    df = stream.to_dataframe()
    extra = pd.DataFrame(
        {
            "user_id": ["u1"],
            "event": ["promo"],
            "timestamp": [pd.Timestamp("2025-01-01")],
            "exp_group": ["treatment"],
        }
    )
    stream = Eventstream(pd.concat([df, extra]), {"segment_cols": ["exp_group"]})
    result = stream.get_conversion_rate(
        "visit", ["order_success", "promo"], diff=("exp_group", "treatment", "control")
    )
    assert result["end_anchor"].tolist() == ["order_success", "promo"]
    promo = result.iloc[1]
    assert promo.group1_converted == 1 and promo.group2_converted == 0
    assert promo.group2_base_rate == 0.0 and np.isnan(promo.group2_lift)


def test_diff_undefined_p_is_nan(stream) -> None:
    row = stream.get_conversion_rate(
        "visit", "catalog", diff=("exp_group", "treatment", "control")
    ).iloc[0]
    assert row.delta == 0.0 and np.isnan(row.p_value)
    assert np.isfinite(row.delta_ci_low)


def test_diff_overlap_raises() -> None:
    df = pd.DataFrame(
        {
            "user_id": ["a", "a", "b"],
            "event": ["add_to_cart", "order_success", "add_to_cart"],
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="h"),
            "exp_group": ["treatment", "control", "control"],
        }
    )
    stream = Eventstream(df, {"segment_cols": ["exp_group"]})
    with pytest.raises(DiffConfigError, match="not constant"):
        stream.get_conversion_rate(
            "add_to_cart", "order_success", diff=("exp_group", "treatment", "control")
        )
    with pytest.raises(DiffConfigError):
        stream.get_conversion_rate(
            "add_to_cart", "order_success", diff=(["a"], ["a", "b"])
        )


def test_mcp_mirrors_the_columns(stream) -> None:
    session = ReportSession(stream, {})
    result = tools.get_conversion_rate(
        session,
        "add_to_cart",
        "order_success",
        ci_level=0.95,
        diff=["exp_group", "treatment", "control"],
    )
    json.dumps(result, allow_nan=False)
    row = result["rows"][0]
    assert set(row) == set(DIFF_COLUMNS)
    assert isinstance(row["group1_converted"], int)
    assert result["diff"] == ["exp_group", "treatment", "control"]
    single = tools.get_conversion_rate(
        session, "add_to_cart", "order_success", ci_level=0.95
    )
    assert "conversion_rate_ci_low" in single["rows"][0]
    undefined = tools.get_conversion_rate(
        session, "visit", "catalog", diff=["exp_group", "treatment", "control"]
    )
    assert undefined["rows"][0]["p_value"] is None


def test_diff_against_itself_raises(stream) -> None:
    with pytest.raises(DiffConfigError, match="itself"):
        stream.get_conversion_rate(
            "add_to_cart",
            "order_success",
            diff=("exp_group", "treatment", "treatment"),
        )


def test_diff_flags_conditional_rows(stream) -> None:
    out = stream.get_conversion_rate(
        ["path_start", "add_to_cart", {"pattern": "path_start", "offset": 1}],
        ["order_success", "catalog"],
        diff=("exp_group", "treatment", "control"),
    )
    assert out["conditional"].tolist() == [False, False, True, True, True, True]
    assert out.loc[~out.conditional, "p_value"].notna().any()
    assert out.loc[out.conditional, "p_value"].isna().all()
