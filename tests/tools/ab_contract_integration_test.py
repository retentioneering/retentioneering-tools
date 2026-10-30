"""Regression contracts at the boundary of the two experiment implementations.

Use hand-counted cohorts and deliberately quiet observation periods: logging
completeness, subgroup eligibility and randomization units are inputs, not facts
inferred from an outcome or from a bootstrap draw.
"""

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    InvalidMetricConfigError,
    InvalidParameterError,
)
from retentioneering.tools.ab_test import ABTest

DIFF = ("arm", "treatment", "control")
CONV = {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order"}}


def cohort():
    rows, roster = [], []
    for arm in ("control", "treatment"):
        for i in range(12):
            user = f"{arm}{i}"
            roster.append(
                (user, arm, pd.Timestamp("2024-01-01"), "web" if i % 2 else "ios")
            )
            rows.append((user, "visit", pd.Timestamp("2024-01-02"), arm, float(i + 1)))
            if i < (4 if arm == "control" else 7):
                rows.append(
                    (user, "order", pd.Timestamp("2024-01-03"), arm, float(i + 1))
                )
    stream = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "value"]),
        {"segment_cols": ["arm"], "custom_cols": ["value"]},
    )
    roster = pd.DataFrame(
        roster, columns=["user_id", "arm", "assigned_at", "platform"]
    ).set_index("user_id")
    return stream, roster


@pytest.mark.parametrize(
    "window", [{"horizon": "7D"}, {"period": "7D", "period_basis": "since_assignment"}]
)
def test_complete_observation_is_not_inferred_from_last_event(window):
    stream, assignment = cohort()
    with pytest.raises(InvalidParameterError, match="observation_end"):
        ABTest.from_stream(stream, DIFF, [CONV], assignment=assignment, **window)


def test_quiet_tail_closes_horizon_and_preserves_all_zero_periods():
    stream, assignment = cohort()
    row = (
        ABTest.from_stream(
            stream,
            DIFF,
            [CONV],
            assignment=assignment,
            horizon="7D",
            observation_end="2024-01-22",
            min_support=1,
        )
        .fit()
        .iloc[0]
    )
    assert (row.group1_paths, row.group2_paths, row.immature_paths) == (12, 12, 0)
    assert row.delta == pytest.approx(7 / 12 - 4 / 12)
    assert row.observation_basis == "declared_logging_cutoff"
    assert row.analysis_unit == "user_id"
    out = ABTest.from_stream(
        stream,
        DIFF,
        [CONV],
        assignment=assignment,
        period="7D",
        period_basis="since_assignment",
        observation_end="2024-01-22",
        min_support=1,
    ).fit()
    assert list(out.period) == ["0-6", "7-13", "14-20"]
    assert list(out.group1_value) == [7 / 12, 0, 0]
    assert list(out.group2_value) == [4 / 12, 0, 0]
    assert list(out.group1_paths) == [12, 12, 12]
    assert out.period_complete.all()


def test_exclusive_cutoff_drops_boundary_events_without_changing_roster():
    stream, assignment = cohort()
    out = ABTest.from_stream(
        stream, DIFF, [CONV], assignment=assignment, observation_end="2024-01-03"
    ).fit()
    assert out.group1_paths.iloc[0] == 12
    assert out.group1_value.iloc[0] == out.group2_value.iloc[0] == 0


def test_cutoff_must_follow_assignments():
    stream, assignment = cohort()
    with pytest.raises(InvalidParameterError, match="after every compared assignment"):
        ABTest.from_stream(
            stream, DIFF, [CONV], assignment=assignment, observation_end="2024-01-01"
        )


@pytest.mark.parametrize("bad", [None, "NaT"])
def test_assignment_timestamp_cannot_be_missing(bad):
    stream, assignment = cohort()
    assignment["assigned_at"] = assignment.assigned_at.astype(object)
    assignment.loc["control0", "assigned_at"] = bad
    with pytest.raises(InvalidParameterError, match="assigned_at"):
        ABTest.from_stream(stream, DIFF, [CONV], assignment=assignment)


def test_null_assignment_identifier_is_rejected():
    stream, assignment = cohort()
    assignment = assignment.rename(index={"control0": None})
    with pytest.raises(InvalidParameterError, match="missing unit identifiers"):
        ABTest.from_stream(stream, DIFF, [CONV], assignment=assignment)


def test_pre_assignment_arm_history_is_ignored_but_post_assignment_conflict_is_not():
    stream, assignment = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    history = frame.iloc[[0]].copy()
    history["arm"] = "treatment"
    history["timestamp"] = pd.Timestamp("2023-12-31")
    frame = pd.concat([frame, history], ignore_index=True)
    schema = {"segment_cols": ["arm"], "custom_cols": ["value"]}
    with_history = Eventstream(frame, schema)
    actual = ABTest.from_stream(with_history, DIFF, [CONV], assignment=assignment).fit()
    expected = ABTest.from_stream(stream, DIFF, [CONV], assignment=assignment).fit()
    np.testing.assert_array_equal(actual.delta, expected.delta)
    assert (
        with_history.get_sample_ratio("arm", assignment=assignment)[
            "multi_valued_paths"
        ]
        == 0
    )
    frame.loc[frame.index[-1], "timestamp"] = pd.Timestamp("2024-01-02")
    conflict = Eventstream(frame, schema)
    with pytest.raises(DiffConfigError, match="not constant"):
        ABTest.from_stream(conflict, DIFF, [CONV], assignment=assignment)
    assert (
        conflict.get_sample_ratio("arm", assignment=assignment)["multi_valued_paths"]
        == 1
    )


def test_explicitly_named_silent_cohort_has_structural_zero_totals():
    stream, assignment = cohort()
    assignment.index = pd.Index(
        ["silent_" + str(i) for i in range(len(assignment))], name="user_id"
    )
    total = {
        "name": "revenue",
        "metric": "event_value_sum",
        "metric_args": {"event": "order", "value_col": "value"},
    }
    with pytest.warns(UserWarning, match="only 0 of 24"):
        out = ABTest.from_stream(
            stream, DIFF, [CONV, total], assignment=assignment
        ).fit()
    assert (out.group1_paths == 12).all() and (out.group2_paths == 12).all()
    assert (out.group1_silent_paths == 12).all() and (
        out.group2_silent_paths == 12
    ).all()
    assert (out.group1_value == 0).all() and (out.group2_value == 0).all()


def test_missing_silent_stratum_remains_an_explicit_row():
    stream, assignment = cohort()
    silent = pd.DataFrame(
        {
            "arm": ["control", "treatment"],
            "assigned_at": pd.Timestamp("2024-01-01"),
            "platform": [None, None],
        },
        index=["silent_c", "silent_t"],
    )
    assignment = pd.concat([assignment, silent])
    out = ABTest.from_stream(
        stream,
        DIFF,
        [CONV],
        assignment=assignment,
        strata_col="platform",
        strata_pre_exposure=True,
    ).fit()
    missing = out.set_index("stratum").loc["<MISSING>"]
    assert (missing.group1_paths, missing.group2_paths) == (1, 1)
    assert out.group1_paths.sum() == out.group2_paths.sum() == 13


@pytest.mark.parametrize("declared", [None, False])
def test_unknown_or_post_exposure_strata_cannot_generate_confirmatory_tests(declared):
    stream, assignment = cohort()
    kwargs = {
        "assignment": assignment,
        "strata_col": "platform",
        "strata_pre_exposure": declared,
        "min_support": 1,
    }
    if declared is None:
        with pytest.warns(UserWarning, match="fixed before exposure"):
            out = ABTest.from_stream(stream, DIFF, [CONV], **kwargs).fit()
    else:
        out = ABTest.from_stream(stream, DIFF, [CONV], **kwargs).fit()
    assert set(out.verdict) == {"descriptive"}
    for column in (
        "p_value",
        "p_value_adjusted",
        "balance_p_value",
        "heterogeneity_p_value",
        "stratum_vs_rest_p_value",
        "mde",
    ):
        assert out[column].isna().all(), column
    assert out.delta.notna().all() and out.delta_ci_low.notna().all()
    assert out.reading.str.contains("pre-exposure membership").all()
    asserted = ABTest.from_stream(
        stream, DIFF, [CONV], **{**kwargs, "strata_pre_exposure": True}
    ).fit()
    assert asserted.p_value.notna().all()


@pytest.mark.parametrize("bad_denominator", [-1.0, np.inf])
def test_ratio_denominators_must_be_finite_nonnegative(bad_denominator):
    stream, _ = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    frame["value"] = bad_denominator
    stream = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    ratio = {
        "name": "ratio",
        "metric": "length",
        "denominator": {"metric": "column_sum", "metric_args": {"column": "value"}},
    }
    with pytest.raises(
        InvalidMetricConfigError, match="denominators must be finite and nonnegative"
    ):
        ABTest.from_stream(stream, DIFF, [ratio])


def test_undefined_ratio_numerator_excludes_its_denominator_together():
    stream, _ = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    frame.loc[frame.user_id == "control0", "value"] = np.nan
    stream = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    ratio = {
        "name": "ratio",
        "metric": "column_mean",
        "metric_args": {"column": "value"},
        "denominator": {"metric": "length"},
    }
    test = ABTest.from_stream(stream, DIFF, [ratio], min_support=1)
    numerator, denominator = test.matrix.values["ratio"]
    pos = test.matrix.path_ids.get_loc("control0")
    assert numerator[pos, 0] == denominator[pos, 0] == 0
    assert test.fit().group2_support.iloc[0] == 11


def test_srm_reports_its_sampling_assumption_and_rejects_nan_shares():
    stream, assignment = cohort()
    result = stream.get_sample_ratio(assignment=assignment)
    assert result["sampling_assumption"] == "independent_bernoulli"
    assert result["p_value_method"] == "exact_binomial_two_sided"
    with pytest.raises(InvalidParameterError, match="finite positive"):
        stream.get_sample_ratio(
            assignment=assignment,
            expected_shares={"control": np.nan, "treatment": np.nan},
        )


def test_typed_arm_labels_are_not_compared_by_string_representation():
    stream, assignment = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    rename = {"control": "1", "treatment": 1}
    frame["arm"] = frame.arm.astype(object).map(rename)
    assignment["arm"] = assignment.arm.map(rename)
    stream = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    diff = ("arm", 1, "1")
    assert ABTest.from_stream(
        stream, diff, [CONV], assignment=assignment
    ).fit().delta.iloc[0] == pytest.approx(3 / 12)
    exported = ABTest.from_stream(stream, diff, [CONV], assignment=assignment).paths()
    assert exported.loc["treatment0", "arm"] == 1
    assert exported.loc["control0", "arm"] == "1"
    assignment.loc["treatment0", "arm"] = "1"
    with pytest.raises(DiffConfigError, match="disagrees"):
        ABTest.from_stream(stream, diff, [CONV], assignment=assignment)
    assert (
        stream.get_sample_ratio("arm", assignment=assignment)["multi_valued_paths"] == 1
    )


def test_signed_ratio_numerators_are_supported():
    stream, _ = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    frame["value"] *= -1
    stream = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    ratio = {
        "name": "refund_per_event",
        "test": "bootstrap",
        "metric": "column_sum",
        "metric_args": {"column": "value"},
        "denominator": {"metric": "length"},
    }
    row = ABTest.from_stream(stream, DIFF, [ratio], min_support=1).fit().iloc[0]
    assert row.group1_value < 0 and row.group2_value < 0
    assert row.group1_support == row.group2_support == 12
    assert row.support_warning != "no_data"
    assert np.isfinite(row.p_value)
    assert np.isnan(row.rel_delta)


def test_contrast_summary_respects_subgroup_contract_and_declared_family():
    stream, assignment = cohort()
    metrics = [
        CONV,
        {"name": "orders", "metric": "event_count", "metric_args": {"event": "order"}},
    ]
    common = {
        "assignment": assignment,
        "period": "7D",
        "period_basis": "since_assignment",
        "observation_end": "2024-01-15",
        "n_bootstrap": 200,
        "min_support": 1,
    }
    contrast = {"early_late": (["0-6"], ["7-13"])}
    test = ABTest.from_stream(stream, DIFF, metrics, **common)
    summary = test.contrast_summary(contrast)
    from retentioneering.tools._stats import holm

    np.testing.assert_allclose(summary.p_value_adjusted, holm(summary.p_value))
    assert set(summary.correction_family) == {"all_metrics/contrasts"}
    assert set(summary.ci_scope) == {"pointwise"}
    assert set(summary.inference_status) == {"ok"}
    assert (
        summary.attrs["bootstrap_algorithm"]
        == test.bootstrap(contrast).attrs["bootstrap_algorithm"]
    )
    descriptive = ABTest.from_stream(
        stream,
        DIFF,
        metrics,
        **common,
        strata_col="platform",
        strata_pre_exposure=False,
    ).contrast_summary(contrast)
    assert (
        descriptive.p_value.isna().all() and descriptive.p_value_adjusted.isna().all()
    )
    assert set(descriptive.inference_status) == {"descriptive"}


def test_degenerate_contrast_has_no_point_interval():
    stream, assignment = cohort()
    test = ABTest.from_stream(
        stream,
        DIFF,
        [CONV],
        assignment=assignment,
        period="7D",
        period_basis="since_assignment",
        observation_end="2024-01-22",
        n_bootstrap=200,
    )
    summary = test.contrast_summary({"quiet_late": (["7-13"], ["14-20"])})
    assert summary.delta.iloc[0] == 0
    assert summary.delta_ci_low.isna().all() and summary.p_value.isna().all()
    assert set(summary.interval_status) == {"degenerate_distribution"}


@pytest.mark.parametrize("paired_variation", [False, True, "decimal"])
def test_decimal_constant_paired_contrast_cannot_turn_roundoff_into_significance(
    paired_variation,
):
    rows, roster = [], []
    for arm in ("control", "treatment"):
        for user in range(30):
            uid = f"{arm}{user}"
            base = (
                (user + 1) / 10
                if paired_variation == "decimal"
                else (float(user % 4) if paired_variation else 0.1)
            )
            change = (
                (1.0 if arm == "control" else 2.0)
                if paired_variation is True
                else (0.1 if arm == "control" else 0.2)
            )
            roster.append((uid, arm, pd.Timestamp("2026-01-01")))
            for day, value in (("2026-01-01", base), ("2026-01-02", base + change)):
                rows.append((uid, "amount", pd.Timestamp(day), arm, value))
    stream = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "value"]),
        {"segment_cols": ["arm"], "custom_cols": ["value"]},
    )
    assignment = pd.DataFrame(
        roster, columns=["user_id", "arm", "assigned_at"]
    ).set_index("user_id")
    metric = {
        "name": "amount",
        "metric": "column_sum",
        "metric_args": {"column": "value"},
    }
    summary = ABTest.from_stream(
        stream,
        DIFF,
        [metric],
        assignment=assignment,
        period="D",
        observation_end="2026-01-03",
        n_bootstrap=500,
    ).contrast_summary({"change": (["2026-01-02"], ["2026-01-01"])})
    assert summary.delta.iloc[0] == pytest.approx(
        1.0 if paired_variation is True else 0.1
    )
    assert summary.delta_ci_low.isna().all() and summary.p_value.isna().all()
    assert set(summary.interval_status) == {"degenerate_distribution"}


def test_sparse_nested_units_are_rejected_for_segment_ids_and_roster():
    rows = []
    for user in range(4):
        arm = "control" if user < 2 else "treatment"
        for session in range(10):
            rows.append(
                (
                    f"u{user}",
                    f"u{user}s{session}",
                    "order" if user % 2 else "visit",
                    pd.Timestamp("2024-01-02"),
                    arm,
                )
            )
    stream = Eventstream(
        pd.DataFrame(
            rows, columns=["user_id", "session_id", "event", "timestamp", "arm"]
        ),
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]},
    )
    assignment = stream.df.groupby("session_id", observed=True).arm.first()
    ids = (
        assignment.index[assignment == "treatment"],
        assignment.index[assignment == "control"],
    )
    for diff, roster in ((DIFF, None), (ids, None), (DIFF, assignment)):
        with pytest.raises(
            InvalidParameterError, match="Intervals assume independent paths"
        ):
            ABTest.from_stream(
                stream, diff, [CONV], assignment=roster, path_col="session_id"
            )
    with pytest.raises(
        InvalidParameterError, match="Intervals assume independent paths"
    ):
        stream.get_sample_ratio("arm", assignment=assignment, path_col="session_id")


def test_strict_report_also_rejects_nested_session_inference():
    from retentioneering.tools.experiment import experiment_data

    stream, assignment = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    frame["session_id"] = frame.user_id.astype(str) + "_session"
    nested = Eventstream(
        frame,
        {
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["arm"],
            "custom_cols": ["value"],
        },
    )
    assignment = assignment.reset_index()
    assignment["session_id"] = assignment.user_id + "_session"
    with pytest.raises(
        InvalidParameterError, match="Intervals assume independent paths"
    ):
        experiment_data(
            nested,
            assignments=assignment,
            metrics=[{**CONV, "agg": "mean"}],
            observation={
                "start": "2024-01-01",
                "end": "2024-01-15",
                "assignment_time_col": "assigned_at",
                "complete": True,
            },
            unit_col="session_id",
            variant_col="arm",
        )


def test_active_periods_suppress_balance_even_with_pre_exposure_strata():
    stream, assignment = cohort()
    out = ABTest.from_stream(
        stream,
        DIFF,
        [CONV],
        assignment=assignment,
        period="D",
        observation_end="2024-01-04",
        strata_col="platform",
        strata_pre_exposure=True,
        period_population="active",
        min_support=1,
    ).fit()
    assert set(out.verdict) == {"descriptive"}
    for column in (
        "p_value",
        "balance_p_value",
        "heterogeneity_p_value",
        "stratum_vs_rest_p_value",
    ):
        assert out[column].isna().all(), column


def test_missing_compared_event_time_is_unknown_window_eligibility():
    stream, assignment = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    frame.loc[(frame.user_id == "control0") & (frame.event == "order"), "timestamp"] = (
        pd.NaT
    )
    bad = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    with pytest.raises(InvalidParameterError, match="window eligibility"):
        bad.ab_test_data(
            DIFF, [CONV], assignment=assignment, observation_end="2024-01-22"
        )


def test_missing_time_for_outside_roster_event_does_not_change_the_comparison():
    stream, assignment = cohort()
    frame = stream.to_dataframe(exclude_start_end=False)
    extra = pd.DataFrame(
        {
            "user_id": ["outside"],
            "event": ["order"],
            "timestamp": [pd.NaT],
            "arm": ["unrelated"],
            "value": [5.0],
        }
    )
    frame = pd.concat([frame, extra], ignore_index=True)
    outside = Eventstream(frame, {"segment_cols": ["arm"], "custom_cols": ["value"]})
    row = outside.ab_test_data(
        DIFF, [CONV], assignment=assignment, observation_end="2024-01-22"
    ).iloc[0]
    assert row.group2_value == pytest.approx(4 / 12)
    assert row.delta == pytest.approx(3 / 12)
