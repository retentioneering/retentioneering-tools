"""Execute the published snippets against small adversarial event logs."""

from pathlib import Path
import re

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream

ROOT = Path(__file__).resolve().parents[2]
SKILL = Path("skills/retentioneering-product-analytics/references")


def run_recipe(number, stream):
    text = (ROOT / ".agents" / SKILL / "analysis-recipes.md").read_text()
    section = text.split(f"## R{number} ·", 1)[1].split("\n## ", 1)[0]
    code = re.search(r"```python\n(.*?)```", section, re.DOTALL).group(1)
    namespace = {"stream": stream}
    exec(compile(code, f"analysis-recipes.md:R{number}", "exec"), namespace)
    return namespace


def make_stream(paths):
    rows = [
        (user, event, pd.Timestamp("2026-01-01") + pd.Timedelta(minutes=i))
        for user, events in enumerate(paths)
        for i, event in enumerate(events)
    ]
    return Eventstream(pd.DataFrame(rows, columns=["user_id", "event", "timestamp"]))


def test_references_are_identical():
    for name in ("analysis-recipes.md", "api-map.md", "gotchas-and-validation.md"):
        assert (ROOT / ".agents" / SKILL / name).read_bytes() == (
            ROOT / ".claude" / SKILL / name
        ).read_bytes()


def test_multi_event_metric_argument():
    stream = make_stream([["basket", "checkout"], ["basket"], ["other"]])
    result = stream.get_metrics(
        [
            {"metric": metric, "metric_args": {"events": ["basket", "checkout"]}}
            for metric in ["has_any_event", "has_all_events"]
        ]
    )
    assert result.sort_index().astype(int).to_numpy().tolist() == [
        [1, 1],
        [1, 0],
        [0, 0],
    ]


def test_ordered_completion_timing():
    stream = make_stream(
        [
            ["checkout", "basket", "help", "checkout"],
            ["checkout", "basket"],
            ["basket", "checkout"],
            ["basket", "help"],
            ["checkout"],
        ]
    )
    result = run_recipe(7, stream)
    assert sorted(result["tb"].iloc[:, 0]) == [60.0, 120.0]
    assert set(result["completed"].to_dataframe()["user_id"]) == {0, 2}


def test_no_ordered_completions_is_explicit():
    with pytest.raises(ValueError, match="No ordered basket-to-checkout completions"):
        run_recipe(7, make_stream([["checkout", "basket"], ["basket", "help"]]))


@pytest.mark.parametrize(
    "paths, expected",
    [
        (
            [
                ["basket", "purchase"],
                ["basket", "help"],
                ["purchase", "basket"],
                ["purchase", "basket", "login", "basket", "purchase"],
                ["other"],
            ],
            0.5,
        ),
        ([["basket"], ["basket", "help", "basket"]], 0.0),
        ([["basket", "purchase"], ["basket", "basket", "purchase"]], 1.0),
    ],
)
def test_markov_population_and_absorption(paths, expected):
    result = run_recipe(9, make_stream(paths))
    assert result["base_conversion"] == pytest.approx(expected)
    assert result["observed"] == pytest.approx(expected)
    assert np.allclose(result["P"].sum(axis=1), 1)
    assert result["P"].loc["purchase", "path_end"] == 0
    assert result["P"].loc["purchase", "purchase"] == 1
    assert result["P"].loc["path_end", "path_end"] == 1


def test_markov_empty_eligibility_is_explicit():
    with pytest.raises(ValueError, match="No eligible basket entrants"):
        run_recipe(9, make_stream([["other"], ["purchase"]]))


@pytest.fixture
def landmark_data():
    rows = []
    for user in range(12):
        for i in range(user + 1):
            rows.append(
                (
                    user,
                    ["listing", "product", "add_to_cart", "search"][user % 4],
                    pd.Timestamp("2026-01-01") + pd.Timedelta(hours=i),
                )
            )
        if user % 2 == 0:
            rows.append((user, "purchase", pd.Timestamp("2026-01-08")))
            rows.append((user, "promo", pd.Timestamp("2026-01-09")))
        elif user != 1:  # User 1 has complete coverage but no future activity.
            rows.append((user, "purchase", pd.Timestamp("2026-01-15")))
    # This eligible ID has no pre-decision history, so cannot be assigned a cluster.
    rows.append((12, "purchase", pd.Timestamp("2026-01-08")))
    return pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])


def test_feature_window_and_outcome_denominator(landmark_data):
    stream = Eventstream(landmark_data)
    result = run_recipe(5, stream)
    features = result["feature_stream"].to_dataframe()
    outcomes = result["outcome_stream"].to_dataframe()
    assert len(features) == sum(range(1, 13))
    assert "promo" not in result["KEY_FAMILIES"]  # Present only after the cutoff.
    assert set(outcomes["event"]) == {"purchase", "promo"}
    assert len(outcomes) == 13
    assert features["timestamp"].max() < result["cutoff"]
    assert result["labeled"].to_dataframe()["user_id"].nunique() == 12
    evaluation = result["evaluation"]
    assert set(evaluation.index) == set(range(12))
    assert set(evaluation.index[evaluation["purchased"]]) == {0, 2, 4, 6, 8, 10}
    assert result["overview"]["paths"].sum() == 12


def test_future_behavior_cannot_change_features_or_labels(landmark_data):
    original = run_recipe(5, Eventstream(landmark_data))
    altered = landmark_data.copy()
    future = altered["timestamp"] >= pd.Timestamp("2026-01-08")
    altered.loc[future, "event"] = "search"
    altered.loc[future, "timestamp"] += pd.Timedelta(days=30)
    result = run_recipe(5, Eventstream(altered))
    pd.testing.assert_frame_equal(
        original["feature_stream"].get_metrics(original["FEATURES"]),
        result["feature_stream"].get_metrics(result["FEATURES"]),
    )
    pd.testing.assert_series_equal(original["labels"], result["labels"])
    assert result["overview"]["paths"].sum() == 12
    assert not result["evaluation"]["purchased"].any()


def test_no_pre_decision_history_is_explicit():
    df = pd.DataFrame(
        {
            "user_id": [0],
            "event": ["purchase"],
            "timestamp": [pd.Timestamp("2026-01-08")],
        }
    )
    with pytest.raises(ValueError, match="No pre-decision history"):
        run_recipe(5, Eventstream(df))


@pytest.mark.parametrize(
    "paths, end, within",
    [
        ([["purchase", "basket"], ["basket", "purchase"]], "purchase", None),
        ([["basket", "help", "purchase"], ["basket", "purchase"]], "purchase", 1),
        (
            [["basket", "purchase"], ["basket", "help", "purchase"]],
            {"pattern": "purchase", "offset": -1},
            1,
        ),
    ],
)
def test_lift_baseline_ignores_order_window_and_offset(paths, end, within):
    result = (
        make_stream(paths).get_conversion_rate("basket", end, within=within).iloc[0]
    )
    assert result["paths_with_start"] == 2
    assert result["base_rate"] == 1.0
    assert result["conversion_rate"] == 0.5
    assert result["lift"] == 0.5


@pytest.mark.parametrize("recipe, outcome", [(7, "checkout"), (9, "purchase")])
def test_shared_index_is_rejected_before_anchor_truncation(recipe, outcome):
    # Valid event order, but truncate_paths currently resolves index-only bounds.
    df = pd.DataFrame(
        {
            "user_id": [0, 0, 0],
            "event": [outcome, "basket", outcome if recipe == 7 else "help"],
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="min"),
            "index": [1, 1, 2],
            "subindex": [1.0, 2.0, 2.0],
        }
    )
    with pytest.raises(ValueError, match="requires a unique index per path"):
        run_recipe(recipe, Eventstream(df))


@pytest.mark.parametrize("boundary", ["path_start", "path_end"])
def test_markov_boundary_name_collision_is_explicit(boundary):
    stream = make_stream([["basket", boundary, "purchase"], ["basket", "help"]])
    with pytest.raises(ValueError, match="reserved boundary names"):
        run_recipe(9, stream)


def test_ordered_completion_with_equal_timestamps():
    df = pd.DataFrame(
        {
            "user_id": [0, 0, 0, 1, 1],
            "event": ["checkout", "basket", "checkout", "checkout", "basket"],
            "timestamp": [pd.Timestamp("2026-01-01")] * 5,
        }
    )
    result = run_recipe(7, Eventstream(df))
    assert set(result["tb"].index) == {0}
    assert result["tb"].iloc[0, 0] == 0.0


def test_landmark_window_boundaries(landmark_data):
    extra = pd.DataFrame(
        [
            (1, "purchase", pd.Timestamp("2026-01-08") - pd.Timedelta("1ns")),
            (3, "purchase", pd.Timestamp("2026-01-08")),
            (5, "purchase", pd.Timestamp("2026-01-15") - pd.Timedelta("1ns")),
            (7, "purchase", pd.Timestamp("2026-01-15")),
        ],
        columns=landmark_data.columns,
    )
    result = run_recipe(5, Eventstream(pd.concat([landmark_data, extra])))
    purchased = result["evaluation"]["purchased"]
    assert not purchased.loc[1]  # Prior purchase is a feature, not a future outcome.
    assert purchased.loc[3]
    assert purchased.loc[5]
    assert not purchased.loc[7]
    assert len(purchased) == 12


def test_markov_repeated_edges_are_counted_with_multiplicity():
    result = run_recipe(
        9,
        make_stream(
            [
                ["basket", "a", "b", "a", "b", "a", "purchase"],
                ["basket", "a", "b", "help"],
            ]
        ),
    )
    assert result["counts"].loc["a", "b"] == 3
    assert result["counts"].loc["b", "a"] == 2
    assert result["base_conversion"] == pytest.approx(0.5)
