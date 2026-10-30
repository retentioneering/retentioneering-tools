"""End-to-end parity at the boundary of the two explicit experiment contracts."""

import copy
import json

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession


@pytest.fixture
def analysis():
    roster = pd.DataFrame(
        {
            "user_id": [f"{arm}{i:02}" for arm in "ct" for i in range(30)],
            "variant": [arm for arm in "ct" for _ in range(30)],
            "assigned_at": pd.Timestamp("2026-01-01"),
        }
    )
    rows = []
    for arm in "ct":
        for i in range(24):
            user = f"{arm}{i:02}"
            rows.append((user, "open", pd.Timestamp("2026-01-02"), 0.0))
            if i < (12 if arm == "c" else 18):
                for order in range(1 + i % 3):
                    rows.append(
                        (
                            user,
                            "buy",
                            pd.Timestamp("2026-01-03") + pd.Timedelta(minutes=order),
                            float((i + 1) * 10),
                        )
                    )
    events = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "amount"])
    stream = Eventstream(events, {"custom_cols": ["amount"]})
    return stream, roster


def strict_result(stream, roster, metrics, **kwargs):
    return stream.experiment_data(
        assignments=roster,
        metrics=metrics,
        variant_col="variant",
        control="c",
        treatment="t",
        observation={
            "start": "2026-01-01",
            "end": "2026-01-15",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        **kwargs,
    )


def test_public_adapters_agree_on_matching_estimands_and_unit_bootstrap(analysis):
    stream, roster = analysis
    before = roster.copy(deep=True)
    revenue = {
        "metric": "event_value_sum",
        "metric_args": {"event": "buy", "value_col": "amount"},
    }
    count = {"metric": "event_count", "metric_args": {"event": "buy"}}
    strict_metrics = [
        {"name": "revenue", **revenue, "agg": "mean"},
        {
            "name": "aov",
            "agg": "ratio_of_sums",
            "numerator": revenue,
            "denominator": count,
        },
    ]
    flex_metrics = [
        {"name": "revenue", **revenue},
        {"name": "aov", **revenue, "denominator": count},
    ]
    original_metrics = copy.deepcopy(flex_metrics)
    strict = strict_result(
        stream,
        roster,
        strict_metrics,
        bootstrap={"n_resamples": 999, "random_state": 42},
    )
    flexible = stream.ab_test_data(
        ("arm", "t", "c"),
        flex_metrics,
        assignment=roster.rename(columns={"variant": "arm"}).set_index("user_id"),
        observation_end="2026-01-15",
        test="bootstrap",
        n_bootstrap=999,
        min_support=1,
    ).set_index("metric")
    for row in strict["summary"]:
        flex = flexible.loc[row["metric"]]
        assert row["control"] == pytest.approx(flex.group2_value)
        assert row["treatment"] == pytest.approx(flex.group1_value)
        assert row["absolute_effect"] == pytest.approx(flex.delta)
        np.testing.assert_allclose(
            [row["absolute_effect_ci_low"], row["absolute_effect_ci_high"]],
            [flex.delta_ci_low, flex.delta_ci_high],
            rtol=1e-12,
        )
        assert flex.group1_paths == flex.group2_paths == 30
        assert flex.group1_silent_paths == flex.group2_silent_paths == 6
    pd.testing.assert_frame_equal(roster, before)
    assert flex_metrics == original_metrics
    json.dumps(strict, allow_nan=False)


def test_public_binary_intervals_have_same_orientation_and_roster(analysis):
    stream, roster = analysis
    metric = {
        "name": "conversion",
        "metric": "has_event",
        "metric_args": {"event": "buy"},
    }
    strict = strict_result(
        stream,
        roster,
        [{**metric, "agg": "mean"}],
        binary_intervals={"method": "newcombe"},
    )
    flexible = stream.ab_test_data(
        ("arm", "t", "c"),
        [metric],
        assignment=roster.rename(columns={"variant": "arm"}).set_index("user_id"),
        observation_end="2026-01-15",
        min_support=1,
    ).iloc[0]
    row = strict["summary"][0]
    assert flexible.delta == pytest.approx(0.2)
    np.testing.assert_allclose(
        [row["absolute_effect_ci_low"], row["absolute_effect_ci_high"]],
        [flexible.delta_ci_low, flexible.delta_ci_high],
    )


def test_mcp_keeps_explicit_cutoff_and_requires_it_for_horizon(analysis):
    stream, roster = analysis
    assignments = roster.rename(columns={"variant": "arm"}).set_index("user_id")
    session = ReportSession(stream, {}, assignment=assignments)
    metrics = [{"metric": "has_event", "metric_args": {"event": "buy"}}]
    bad = tools.get_ab_test(session, ["arm", "t", "c"], metrics, horizon="7D")
    assert "observation_end" in bad["error"]
    good = tools.get_ab_test(
        session, ["arm", "t", "c"], metrics, horizon="7D", observation_end="2026-01-15"
    )
    assert good["observation_basis"] == "declared_logging_cutoff"
    assert good["observation_end"] == "2026-01-15"
    assert good["analysis_unit"] == "user_id"
    assert good["metrics"][0]["group1_paths"] == 30
    json.dumps(good, allow_nan=False)


def test_repeated_mcp_calls_keep_assignment_diagnostics_self_contained(analysis):
    """Distinct same-size rosters must not share a process-global warning cache."""
    stream, roster = analysis
    for suffix in ("first", "second", "second"):
        silent = pd.DataFrame(
            {
                "user_id": [f"silent_{suffix}_{i}" for i in range(120)],
                "variant": ["c", "t"] * 60,
                "assigned_at": pd.Timestamp("2026-01-01"),
            }
        )
        assignment = (
            pd.concat([roster, silent])
            .rename(columns={"variant": "arm"})
            .set_index("user_id")
        )
        session = ReportSession(stream, {}, assignment=assignment)
        srm = tools.get_sample_ratio(session)
        assert srm["sampling_assumption"] == "independent_multinomial"
        assert any("48 of 180" in warning for warning in srm["warnings"])
        scorecard = tools.get_ab_test(
            session,
            ["arm", "t", "c"],
            [{"metric": "has_event", "metric_args": {"event": "buy"}}],
        )
        assert any("48 of 180" in warning for warning in scorecard["warnings"])
        assert scorecard["metrics"][0]["group1_paths"] == 90
        json.dumps(srm, allow_nan=False)
