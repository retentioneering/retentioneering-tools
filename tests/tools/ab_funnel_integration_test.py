"""Contracts shared by the two integrated A/B proposals and descriptive tools."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream, engine
from retentioneering.exceptions import InvalidMetricConfigError, InvalidParameterError
from retentioneering.metrics.metric_builder import SCALAR_OUTCOME_CAPABILITIES
from retentioneering.paths.ordered_reach import ordered_reach


def nested_stream():
    rows = []
    # Four independent users with two sessions each: below the old 10-user
    # heuristic threshold, but sessions still cannot silently become units.
    for arm in ("control", "treatment"):
        for i in range(2):
            user = f"{arm}{i}"
            for session in range(2):
                sid = f"{user}s{session}"
                for k, event in enumerate(
                    ["open", "buy", "buy"] if i == 0 else ["open"]
                ):
                    rows.append(
                        (
                            user,
                            sid,
                            event,
                            pd.Timestamp("2026-01-03")
                            + pd.Timedelta(minutes=10 * session + k),
                            arm,
                            2.5 if event == "buy" else np.nan,
                        )
                    )
    return Eventstream(
        pd.DataFrame(
            rows,
            columns=["user_id", "session_id", "event", "timestamp", "arm", "amount"],
        ),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["arm"],
            "custom_cols": ["amount"],
        },
    )


@pytest.mark.parametrize(
    "diff",
    [
        None,
        ("arm", "treatment", "control"),
        (["treatment0s0", "treatment1s0"], ["control0s0", "control1s0"]),
    ],
)
def test_finer_grain_funnel_point_estimates_remain_descriptive(diff):
    stream = nested_stream()
    point = stream.funnel_data(["open", "buy"], path_col="session_id", diff=diff)
    assert point["steps"]
    with pytest.raises(InvalidParameterError, match="independent paths"):
        stream.funnel_data(
            ["open", "buy"], path_col="session_id", diff=diff, ci_level=0.95
        )


def test_conversion_rejects_nested_intervals_even_without_segment_diff():
    stream = nested_stream()
    assert len(stream.get_conversion_rate("open", "buy", path_col="session_id")) == 1
    with pytest.raises(InvalidParameterError, match="independent paths"):
        stream.get_conversion_rate("open", "buy", path_col="session_id", ci_level=0.95)
    with pytest.raises(InvalidParameterError, match="independent paths"):
        stream.get_conversion_rate(
            "path_start",
            "buy",
            path_col="session_id",
            diff=(["treatment0s0"], ["control0s0"]),
        )


@pytest.mark.parametrize("ci_level", [None, 0.95])
def test_funnel_reuses_ordered_reach_and_only_materializes_counts(
    ci_level, monkeypatch
):
    stream = nested_stream()
    steps = ["open", "buy", "buy"]
    expected = ordered_reach(stream, steps).sum().to_list()
    shapes = []
    run = engine.run

    def recorded(*args, **kwargs):
        result = run(*args, **kwargs)
        shapes.append(result.shape)
        return result

    monkeypatch.setattr(engine, "run", recorded)
    result = stream.funnel_data(steps, ci_level=ci_level)
    assert [row["unique_paths"] for row in result["steps"]] == expected
    assert shapes == [(1, len(steps))]
    assert result["total_paths"] == 4
    if ci_level is not None:
        assert result["population_basis"] == "observed_paths"
        assert result["steps"][0]["conversion_rate_ci_high"] == 1


def test_named_column_and_event_sum_metrics_share_registry_without_conflating_null_policy():
    stream = nested_stream()
    configs = [
        {
            "name": "all_amounts",
            "metric": "column_sum",
            "metric_args": {"column": "amount"},
        },
        {
            "name": "buy_amounts",
            "metric": "event_value_sum",
            "metric_args": {"event": "buy", "value_col": "amount"},
        },
    ]
    values = stream.get_metrics(configs)
    assert list(values.columns) == ["all_amounts", "buy_amounts"]
    pd.testing.assert_series_equal(
        values.all_amounts, values.buy_amounts, check_names=False
    )
    assert values.buy_amounts.to_dict() == {
        "control0": 10.0,
        "control1": 0.0,
        "treatment0": 10.0,
        "treatment1": 0.0,
    }
    assert "event_value_sum" in SCALAR_OUTCOME_CAPABILITIES
    frame = stream.to_dataframe()
    frame.loc[frame.event.eq("buy"), "amount"] = np.nan
    missing = Eventstream(
        frame,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["arm"],
            "custom_cols": ["amount"],
        },
    )
    assert missing.get_metrics(configs[:1]).all_amounts.eq(0).all()
    with pytest.raises(InvalidMetricConfigError, match="missing|nonfinite"):
        missing.get_metrics(configs[1:])


def test_conditional_conversion_keeps_interval_but_suppresses_pvalue():
    stream = nested_stream()
    result = stream.get_conversion_rate(
        ["path_start", "open"], "buy", diff=("arm", "treatment", "control")
    )
    unconditional, conditional = result.iloc[0], result.iloc[1]
    assert not unconditional.conditional
    assert np.isfinite(unconditional.p_value)
    assert conditional.conditional and np.isnan(conditional.p_value)
    assert np.isfinite(conditional.delta_ci_low)
    assert np.isfinite(conditional.delta_ci_high)
