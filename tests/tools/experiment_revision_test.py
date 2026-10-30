"""Regressions for the reviewed population, outcome and inference contracts."""

import copy
import json
from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest
from retentioneering.tools.experiment import experiment_data, session_funnel_data
from retentioneering import Eventstream
from scipy import stats

OBS = {
    "start": "2026-01-02",
    "end": "2026-01-16",
    "assignment_time_col": "assigned_at",
    "complete": True,
}
CONVERSION = {
    "name": "conversion",
    "metric": "has_event",
    "metric_args": {"event": "buy"},
    "agg": "mean",
}


def binary_case(cs=1, nc=4, ts=2, nt=4):
    rows, roster = [], []
    for arm, successes, n in (("c", cs, nc), ("t", ts, nt)):
        for i in range(n):
            user = f"{arm}{i:05}"
            roster.append((user, arm, pd.Timestamp("2026-01-01")))
            rows.append(
                (user, user, "open", pd.Timestamp("2026-01-03"), arm, "catalog")
            )
            if i < successes:
                rows.append(
                    (
                        user,
                        user,
                        "buy",
                        pd.Timestamp("2026-01-03 00:01"),
                        arm,
                        "checkout",
                    )
                )
    frame = pd.DataFrame(
        rows,
        columns=["user_id", "session_id", "event", "timestamp", "variant", "screen"],
    )
    stream = Eventstream(
        frame,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["screen"],
        },
    )
    return stream, pd.DataFrame(roster, columns=["user_id", "variant", "assigned_at"])


def run(case, **kwargs):
    stream, assignments = case
    args = {
        "assignments": assignments,
        "metrics": [copy.deepcopy(CONVERSION)],
        "observation": OBS,
        "variant_col": "variant",
        "control": "c",
        "treatment": "t",
        "event_catalog": {"event": ["open", "buy"]},
    }
    args.update(kwargs)
    return experiment_data(stream, **args)


@pytest.mark.parametrize("missing", ["NaT", "", pd.NaT])
def test_assignment_time_is_validated_after_parsing(missing):
    stream, assignments = binary_case()
    assignments.assigned_at = assignments.assigned_at.astype(object)
    assignments.loc[0, "assigned_at"] = missing
    with pytest.raises(ValueError, match="nonnull"):
        run((stream, assignments))


def test_preassignment_history_with_unassigned_labels_is_allowed_but_not_outcome():
    stream, assignments = binary_case()
    original = run((stream, assignments)).summary
    df = stream.to_dataframe().drop(columns=["index", "subindex", "event_type"])
    before = df.iloc[:1].copy()
    before["timestamp"] = pd.Timestamp("2025-12-01")
    before["event"] = "buy"
    before["variant"] = None
    enriched = Eventstream(
        pd.concat([before, df], ignore_index=True),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["screen"],
        },
    )
    result = run((enriched, assignments))
    pd.testing.assert_frame_equal(original, result.summary)


def test_synthetic_markers_survive_experiment_and_session_funnel():
    stream, assignments = binary_case()
    stream = stream.add_start_end_events()
    metric = {
        "name": "start",
        "metric": "has_event",
        "metric_args": {"event": "path_start"},
        "agg": "mean",
    }
    steps = ["path_start", "open", "path_end"]
    result = run((stream, assignments), metrics=[metric], funnel_steps=steps)
    native = stream.get_metrics(
        [{"metric": "has_event", "metric_args": {"event": "path_start"}}],
        path_col="user_id",
    )
    assert (
        result.summary.iloc[0].control
        == result.summary.iloc[0].treatment
        == native.iloc[:, 0].mean()
        == 1
    )
    assert (
        result.summary.loc[result.summary.family.eq("funnel_cumulative"), "control"]
        .eq(1)
        .all()
    )
    sessions = session_funnel_data(
        stream, steps=steps, segment_col="variant", control="c", treatment="t"
    )
    assert (
        sessions.summary.loc[sessions.summary.family.eq("funnel_cumulative"), "control"]
        .eq(1)
        .all()
    )


def test_declared_absent_event_is_zero_but_undeclared_typo_fails():
    case = binary_case(0, 4, 0, 4)
    result = run(case, binary_intervals={"method": "newcombe"})
    row = result.summary.iloc[0]
    assert row.control == row.treatment == 0
    assert row.absolute_effect_ci_low < 0 < row.absolute_effect_ci_high
    bad = {**CONVERSION, "metric_args": {"event": "buuy"}}
    with pytest.raises(ValueError, match="catalogue"):
        run(case, metrics=[bad])
    result = run(case, metrics=[], funnel_steps=["open", "buy"])
    assert result.summary.query("metric == 'funnel_cumulative_2'").iloc[0].control == 0


@pytest.mark.parametrize(
    "pattern",
    ["open->.*->buy", "open->[^cancel]*->buy", "[open|buy]", "open->.*->buy->buy"],
)
def test_native_pattern_parity_and_declared_absent_tokens(pattern):
    stream, assignments = binary_case()
    metric = {
        "name": "journey",
        "metric": "matches_pattern",
        "metric_args": {"pattern": pattern},
        "agg": "mean",
    }
    result = run(
        (stream, assignments),
        metrics=[metric],
        event_catalog={"event": ["open", "buy", "cancel"]},
    )
    row = result.summary.iloc[0]
    assert row.outcome_kind == "binary"
    if "cancel" not in pattern:
        native = stream.get_metrics(
            [{"metric": "matches_pattern", "metric_args": {"pattern": pattern}}],
            path_col="user_id",
        ).iloc[:, 0]
        groups = assignments.set_index("user_id").variant
        assert row.control == native[groups.eq("c")].mean()
        assert row.treatment == native[groups.eq("t")].mean()
    else:
        assert row.control == 0.25 and row.treatment == 0.5


def test_pattern_typo_is_rejected_inside_class():
    metric = {
        "name": "journey",
        "metric": "matches_pattern",
        "metric_args": {"pattern": "open->[buy|typo]"},
        "agg": "mean",
    }
    with pytest.raises(Exception, match="typo"):
        run(binary_case(), metrics=[metric])


@pytest.mark.parametrize("pattern", [".*", "[buy]*", ".{0,3}"])
def test_native_parser_rejects_patterns_with_no_anchor(pattern):
    metric = {
        "name": "journey",
        "metric": "matches_pattern",
        "metric_args": {"pattern": pattern},
        "agg": "mean",
    }
    with pytest.raises(Exception, match="pattern|Anchor|anchor"):
        run(binary_case(), metrics=[metric])


@pytest.mark.parametrize("kind", ["has_event", "event_count", "matches_pattern"])
def test_event_column_override_uses_native_registry(kind):
    stream, assignments = binary_case()
    args = {
        "event_col": "screen",
        **(
            {"pattern": "catalog->.*->checkout"}
            if kind == "matches_pattern"
            else {"event": "checkout"}
        ),
    }
    spec = {"metric": kind, "metric_args": args}
    result = run(
        (stream, assignments),
        metrics=[{"name": "screen_outcome", **spec, "agg": "mean"}],
    )
    native = stream.get_metrics([spec], path_col="user_id").iloc[:, 0]
    assert (
        result.summary.iloc[0].control
        == native[native.index.str.startswith("c")].mean()
    )
    assert (
        result.summary.iloc[0].treatment
        == native[native.index.str.startswith("t")].mean()
    )


def test_declared_absent_event_on_custom_column():
    metric = {
        "name": "screen_outcome",
        "metric": "has_event",
        "metric_args": {"event_col": "screen", "event": "receipt"},
        "agg": "mean",
    }
    result = run(binary_case(), metrics=[metric], event_catalog={"screen": ["receipt"]})
    assert result.summary.iloc[0].control == result.summary.iloc[0].treatment == 0


def test_binary_funnel_and_pattern_support_fisher_but_conditional_does_not():
    stream, assignments = binary_case()
    metric = {
        "name": "journey",
        "metric": "matches_pattern",
        "metric_args": {"pattern": "open->.*->buy"},
        "agg": "mean",
    }
    result = run(
        (stream, assignments),
        metrics=[metric],
        funnel_steps=["open", "buy"],
        tests={
            "methods": {
                "journey": "fisher_exact",
                "funnel_cumulative_2": "fisher_exact",
            },
            "adjustment": "holm",
        },
    )
    rows = result.summary.set_index("metric")
    expected = stats.fisher_exact([[2, 2], [1, 3]], alternative="two-sided").pvalue
    assert (
        rows.loc["journey", "p_value"]
        == rows.loc["funnel_cumulative_2", "p_value"]
        == expected
    )
    with pytest.raises(ValueError, match="conditional"):
        run(
            (stream, assignments),
            funnel_steps=["open", "buy"],
            tests={
                "methods": {"funnel_conditional_2": "fisher_exact"},
                "adjustment": "holm",
            },
        )


@pytest.mark.parametrize(
    "cs,nc,ts,nt,expected",
    [
        (70, 98, 84, 101, (0.0008019789961014462, 0.23102801749884483)),
        (0, 10, 300, 1000, (0.02109887533065885, 0.32912386091721724)),
        (0, 10, 0, 10, (-0.27753279986288926, 0.27753279986288926)),
        (0, 10, 10, 10, (0.6075093504305242, 1.0)),
    ],
)
def test_newcombe_reference_and_wilson_boundaries(cs, nc, ts, nt, expected):
    # Reference limits independently derived with scipy.stats.binomtest's
    # Wilson limits and Newcombe's uncorrected score-difference construction.
    result = run(
        binary_case(cs, nc, ts, nt),
        binary_intervals={"method": "newcombe", "confidence_level": 0.95},
    )
    row = result.summary.iloc[0]
    np.testing.assert_allclose(
        [row.absolute_effect_ci_low, row.absolute_effect_ci_high], expected, atol=2e-14
    )
    marginal = stats.binomtest(cs, nc).proportion_ci(method="wilson")
    np.testing.assert_allclose(
        [row.control_ci_low, row.control_ci_high], marginal, atol=2e-14
    )
    assert (
        row.absolute_effect_ci_method == "newcombe_approximate"
        and row.control_ci_method == "wilson_approximate"
    )
    assert (
        row.absolute_effect_ci_confidence_level == 0.95
        and row.absolute_effect_status == "ok"
    )
    assert row.relative_effect_status in {"not_requested", "undefined_baseline"}


def test_boundary_bootstrap_contrast_is_suppressed_without_hidden_fallback():
    result = run(
        binary_case(0, 10, 300, 1000),
        bootstrap={"n_resamples": 199, "random_state": 42},
    )
    row = result.summary.iloc[0]
    assert (
        row.absolute_effect == 0.3
        and row.absolute_effect_status == "degenerate_component"
    )
    assert pd.isna(row.absolute_effect_ci_low)
    assert row.absolute_effect_ci_method == "bootstrap_percentile"
    assert result.metadata["binary_intervals"] is None


def test_selected_binary_method_does_not_change_conditional_or_relative_method():
    result = run(
        binary_case(0, 10, 3, 10),
        bootstrap={"n_resamples": 199, "random_state": 42},
        funnel_steps=["open", "buy"],
        binary_intervals={"method": "newcombe", "confidence_level": 0.9},
    )
    rows = result.summary.set_index("metric")
    assert (
        rows.loc["funnel_cumulative_2", "absolute_effect_ci_method"]
        == "newcombe_approximate"
    )
    assert (
        rows.loc["funnel_conditional_2", "absolute_effect_ci_method"]
        == "bootstrap_percentile"
    )
    assert rows.loc["conversion", "relative_effect_ci_method"] == "bootstrap_percentile"
    assert rows.loc["conversion", "absolute_effect_ci_confidence_level"] == 0.9


def test_session_binary_intervals_are_explicitly_unsupported():
    stream, _ = binary_case()
    with pytest.raises(ValueError, match="independent assigned Bernoulli"):
        session_funnel_data(
            stream,
            steps=["open", "buy"],
            segment_col="variant",
            control="c",
            treatment="t",
            binary_intervals={"method": "newcombe"},
        )


def test_metadata_snapshot_and_display_support_are_self_describing():
    observation = {**OBS, "start": date(2026, 1, 2), "end": datetime(2026, 1, 16)}
    metrics = [
        {
            **copy.deepcopy(CONVERSION),
            "display": {
                "label": "Buyers",
                "unit": "percentage points",
                "scale": 100,
                "direction": "higher_is_better",
                "role": "primary",
            },
        }
    ]
    catalog = {"event": ["open", "buy"]}
    tests = {"methods": {"conversion": "fisher_exact"}, "adjustment": "holm"}
    result = run(
        binary_case(),
        observation=observation,
        metrics=metrics,
        event_catalog=catalog,
        tests=tests,
        binary_intervals={"method": "newcombe"},
    )
    frozen = result.to_dict()
    observation["end"] = "2030-01-01"
    metrics[0]["metric_args"]["event"] = "changed"
    metrics[0]["display"]["label"] = "Changed"
    tests["methods"].clear()
    catalog["event"].append("different")
    assert result.to_dict() == frozen
    assert frozen["schema_version"] == "2.0"
    assert frozen["metadata"]["observation"]["start"] == "2026-01-02"
    assert frozen["metadata"]["observation"]["end"] == "2026-01-16T00:00:00"
    json.dumps(frozen, allow_nan=False)
    row = result.summary.iloc[0]
    assert (
        row.label == "Buyers" and row.unit == "percentage points" and row.scale == 100
    )
    assert row.control_successes == 1 and row.control_failures == 3
    assert row.control_nonzero_units == 1 and row.control_eligible_units == 4
    external = result.to_dict()
    external["metadata"]["metrics"][0]["display"]["label"] = "Cannot alias"
    assert result.to_dict() == frozen


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"method": "auto"},
        {"method": "newcombe", "confidence_level": True},
        {"method": "newcombe", "typo": 1},
    ],
)
def test_binary_interval_configuration_fails_loudly(config):
    with pytest.raises(ValueError):
        run(binary_case(), binary_intervals=config)
