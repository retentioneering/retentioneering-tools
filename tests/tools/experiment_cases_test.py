"""Independent fixture oracles and public-API tutorial integration checks."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks"))

import numpy as np
import pandas as pd
from retentioneering.tools.experiment import experiment_data
from retentioneering import Eventstream
from experiment_demo import (
    CATALOG,
    OBSERVATION,
    ORDERS,
    PATTERN,
    REVENUE,
    SCHEMA,
    exact_ratio_bootstrap,
    make_demo_dataset,
    make_ratio_fixture,
    make_repeated_sessions,
    manual_ordered_reach,
)


def test_predeclared_demo_is_reproducible_and_retains_silent_assignments():
    first, second = make_demo_dataset(), make_demo_dataset()
    for name in ["events", "assignments", "unit_truth"]:
        pd.testing.assert_frame_equal(first[name], second[name])
    assert first["metadata"]["seed"] == 20261029
    assert first["metadata"]["purchase_probability"] == {
        "control": 0.1,
        "treatment": 0.13,
    }
    assert first["assignments"].groupby("variant").size().tolist() == [500, 500]
    assert first["assignments"].user_id.is_unique
    assert (first["unit_truth"].active == 0).any()
    assert (first["unit_truth"].n_orders > 1).any()
    assert (first["unit_truth"].n_sessions > 1).any()
    events = first["events"]
    window = events.loc[
        events.timestamp.ge(OBSERVATION["start"])
        & events.timestamp.lt(OBSERVATION["end"])
    ]
    assert set(first["assignments"].user_id) == set(events.user_id)
    assert set(window.user_id) == set(first["unit_truth"].query("active == 1").user_id)
    orders = window.loc[window.event.eq("order_success")]
    expected = first["unit_truth"].set_index("user_id")
    assert (
        orders.groupby("user_id").size().reindex(expected.index, fill_value=0).tolist()
        == expected.n_orders.tolist()
    )
    np.testing.assert_allclose(
        orders.groupby("user_id")
        .order_value_gbp.sum()
        .reindex(expected.index, fill_value=0),
        expected.revenue,
    )


def test_native_pattern_outcome_matches_independent_generation_flags():
    demo = make_demo_dataset(n_per_arm=50)
    stream = Eventstream(demo["events"], schema=SCHEMA)
    metric = {
        "name": "backtrack",
        "metric": "matches_pattern",
        "metric_args": {"pattern": PATTERN},
        "agg": "mean",
    }
    result = experiment_data(
        stream,
        assignments=demo["assignments"],
        metrics=[metric],
        observation=OBSERVATION,
        variant_col="variant",
        event_catalog=CATALOG,
    )
    expected = demo["unit_truth"].groupby("variant").checkout_backtrack.mean()
    row = result.summary.iloc[0]
    assert row.control == expected["control"]
    assert row.treatment == expected["treatment"]


def test_ratio_estimands_have_opposite_known_signs_via_public_api():
    fixture = make_ratio_fixture()
    metrics = [
        {
            "name": "pooled",
            "agg": "ratio_of_sums",
            "numerator": REVENUE,
            "denominator": ORDERS,
        },
        {
            "name": "personal",
            "agg": "mean_of_ratios",
            "numerator": REVENUE,
            "denominator": ORDERS,
            "eligibility": "denominator_positive",
        },
    ]
    result = experiment_data(
        Eventstream(fixture["events"], schema=SCHEMA),
        assignments=fixture["assignments"],
        observation=OBSERVATION,
        variant_col="variant",
        metrics=metrics,
        event_catalog=CATALOG,
    )
    table = result.summary.set_index("metric")
    np.testing.assert_allclose(
        table.loc[["pooled", "personal"], "control"], [10.0, 10.0]
    )
    np.testing.assert_allclose(
        table.loc[["pooled", "personal"], "treatment"], [8.7, 11.5]
    )
    np.testing.assert_allclose(
        table.loc[["pooled", "personal"], "absolute_effect"], [-1.3, 1.5]
    )


def test_exact_ratio_resampling_oracle():
    samples = exact_ratio_bootstrap()
    assert len(samples) == 16
    assert samples.pooled_effect.round(6).value_counts().to_dict() == {
        -1.3: 8,
        5.0: 4,
        -2.0: 4,
    }
    assert samples.personal_effect.round(6).value_counts().to_dict() == {
        1.5: 8,
        5.0: 4,
        -2.0: 4,
    }


def test_uniform_session_repetition_keeps_cluster_intervals():
    boot = {"n_resamples": 499, "random_state": 73}
    results = []
    for repeat in [1, 100]:
        frame = make_repeated_sessions(repeat)
        stream = Eventstream(frame, schema=SCHEMA)
        assignments = (
            frame[["user_id", "variant"]]
            .drop_duplicates()
            .assign(assigned_at=pd.Timestamp("2026-01-01"))
        )
        results.append(
            experiment_data(
                stream,
                assignments=assignments,
                observation=OBSERVATION,
                variant_col="variant",
                event_catalog=CATALOG,
                bootstrap=boot,
                metrics=[
                    {
                        "name": "session_completion",
                        "agg": "ratio_of_sums",
                        "numerator": ORDERS,
                        "denominator": {
                            "metric": "event_count",
                            "metric_args": {"event": "add_to_cart"},
                        },
                    }
                ],
            )
        )
    cols = [
        "control",
        "treatment",
        "absolute_effect",
        "absolute_effect_ci_low",
        "absolute_effect_ci_high",
    ]
    np.testing.assert_allclose(
        results[0].summary[cols], results[1].summary[cols], equal_nan=True
    )
    assert (
        results[0].diagnostics.n_assigned.tolist()
        == results[1].diagnostics.n_assigned.tolist()
        == [4, 4]
    )
    assert results[0].summary.iloc[0].control_denominator == 4
    assert results[1].summary.iloc[0].control_denominator == 400
    assert results[0].summary.iloc[0].absolute_effect == 0.25
    assert results[0].summary.iloc[0].absolute_effect_status == "ok"


def test_independent_scanner_distinguishes_session_and_calendar_boundaries():
    frame = pd.DataFrame(
        [
            ("u", "s1", "cart", "2026-01-03"),
            ("u", "s2", "order_success", "2026-01-04"),
            ("v", "v1", "cart", "2026-01-01"),
            ("v", "v1", "order_success", "2026-01-03"),
        ],
        columns=["user_id", "session_id", "event", "timestamp"],
    )
    frame.timestamp = pd.to_datetime(frame.timestamp)
    window = frame.loc[frame.timestamp.ge(OBSERVATION["start"])]
    user = manual_ordered_reach(window, ["cart", "order_success"])
    sessions = manual_ordered_reach(window, ["cart", "order_success"], "session_id")
    assert user.loc["u", "order_success"] == 1
    assert user.loc["v", "order_success"] == 0
    assert sessions.order_success.sum() == 0
    assert window.loc[window.event.eq("order_success"), "user_id"].nunique() == 2
