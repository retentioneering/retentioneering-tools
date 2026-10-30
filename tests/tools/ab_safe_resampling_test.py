"""Bounded numeric, eligibility and resource contracts for whole-unit inference."""

import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _bootstrap as bs
from retentioneering.tools.ab_test import ABTest


def stream_for(control, treatment, *, paired=False):
    rows, assignments = [], []
    for arm, values in (("control", control), ("treatment", treatment)):
        values = np.asarray(values, dtype=float)
        if values.ndim == 1:
            values = np.column_stack([values, np.ones(len(values))])
        for i, value in enumerate(values):
            uid = f"{arm}_{i:04d}"
            assignments.append((uid, arm, pd.Timestamp("2026-01-01")))
            for period in range(2 if paired else 1):
                rows.append(
                    (
                        uid,
                        arm,
                        "value",
                        pd.Timestamp("2026-01-01 12:00") + pd.Timedelta(days=period),
                        value[period] if paired else value[0],
                        1.0 if paired else value[1],
                    )
                )
    frame = pd.DataFrame(
        rows, columns=["user_id", "arm", "event", "timestamp", "num", "den"]
    )
    stream = Eventstream(
        frame,
        {"segment_cols": ["arm"], "custom_cols": ["num", "den"]},
        preprocess=False,
    )
    assignment = pd.DataFrame(
        assignments, columns=["user_id", "arm", "assigned_at"]
    ).set_index("user_id")
    return stream, assignment


def metric(ratio=False):
    result = {
        "name": "outcome",
        "metric": "column_mean",
        "metric_args": {"column": "num"},
    }
    if ratio:
        result["denominator"] = {
            "metric": "column_mean",
            "metric_args": {"column": "den"},
        }
    return result


def args(assignment, ratio=False):
    return {
        "diff": ("arm", "treatment", "control"),
        "metrics": [metric(ratio)],
        "assignment": assignment,
        "n_bootstrap": 199,
        "random_state": 3127,
    }


@pytest.mark.parametrize("ratio", [False, True])
@pytest.mark.parametrize("test", ["auto", "bootstrap"])
def test_finite_overflow_rejected_on_scorecard(ratio, test):
    c, t = np.arange(1.0, 41.0) * 1e306, np.arange(2.0, 42.0) * 1e306
    if ratio:
        c, t = np.column_stack([c, c + 3e306]), np.column_stack([t, t + 3e306])
    stream, assignment = stream_for(c, t)
    with pytest.raises(ValueError, match="[Nn]umeric overflow"):
        stream.get_ab_test(**args(assignment, ratio), test=test)


@pytest.mark.parametrize("ratio", [False, True])
@pytest.mark.parametrize(
    "bootstrap", [None, {"n_resamples": 199, "random_state": 3127}]
)
def test_finite_overflow_rejected_on_strict_report(ratio, bootstrap):
    c, t = np.arange(1.0, 41.0) * 1e306, np.arange(2.0, 42.0) * 1e306
    if ratio:
        c, t = np.column_stack([c, c + 3e306]), np.column_stack([t, t + 3e306])
    stream, assignment = stream_for(c, t)

    def component(col):
        return {
            "metric": "event_value_sum",
            "metric_args": {"event": "value", "value_col": col},
        }

    spec = {"name": "outcome", "agg": "mean", **component("num")}
    if ratio:
        spec = {
            "name": "outcome",
            "agg": "ratio_of_sums",
            "numerator": component("num"),
            "denominator": component("den"),
        }
    with pytest.raises(ValueError, match="[Nn]umeric overflow"):
        stream.experiment_data(
            assignments=assignment.reset_index(),
            variant_col="arm",
            metrics=[spec],
            bootstrap=bootstrap,
            observation={
                "start": "2026-01-01",
                "end": "2026-01-03",
                "assignment_time_col": "assigned_at",
                "complete": True,
            },
        )


def test_finite_overflow_rejected_on_raw_kernel():
    values = np.r_[np.arange(1.0, 41.0), np.arange(2.0, 42.0)] * 1e306
    with pytest.raises(ValueError, match="[Nn]umeric overflow"):
        bs.bootstrap_paths(values, None, np.repeat([0, 1], 40), 199)


def test_weighted_resample_overflow_rejected_when_original_sum_is_finite():
    values = np.array([1e308, 0.0, 1e308, 0.0])
    with pytest.raises(ValueError, match="[Nn]umeric overflow"):
        bs.bootstrap_paths(values, None, np.array([0, 0, 1, 1]), 199)


def test_replicate_by_contribution_resource_limit_precedes_allocation():
    with pytest.raises(ValueError, match="[Rr]esource limit"):
        bs.bootstrap_paths(np.ones((4, 101)), None, np.array([0, 0, 1, 1]), 100_000)


@pytest.mark.parametrize("summary", [False, True])
def test_changing_measurement_eligibility_rejected_for_period_contrast(summary):
    c = np.tile([[1.0, 2.0], [100.0, np.nan], [np.nan, 100.0]], (10, 1))
    t = np.tile([[2.0, 3.0], [200.0, np.nan], [np.nan, 400.0]], (10, 1))
    stream, assignment = stream_for(c, t, paired=True)
    options = {**args(assignment), "period": "D"}
    contrast = {"change": (["2026-01-02"], ["2026-01-01"])}
    with pytest.raises(InvalidParameterError, match="outcome eligibility"):
        if summary:
            ABTest.from_stream(stream, **options).contrast_summary(contrast)
        else:
            stream.get_ab_bootstrap(**options, contrast=contrast)


def test_complete_paired_change_retains_shared_weights():
    base = np.arange(1.0, 41.0)
    c, t = np.column_stack([base, base + 2]), np.column_stack([base * 9, base * 9 + 5])
    stream, assignment = stream_for(c, t, paired=True)
    draws = stream.get_ab_bootstrap(
        **args(assignment),
        period="D",
        contrast={"change": (["2026-01-02"], ["2026-01-01"])},
    )
    assert np.allclose(draws.loc[draws.contrast.eq("change"), "delta"], 3.0)


def test_legitimate_zero_ratio_denominators_do_not_imply_missing_pairs():
    # Observed denominators can change from zero to positive between periods.
    c = np.tile([[1.0, 2.0], [4.0, 5.0], [7.0, 8.0]], (12, 1))
    stream, assignment = stream_for(c, c + 1, paired=True)
    frame = stream.to_dataframe(exclude_start_end=False)
    frame["den"] = np.where(frame.timestamp.dt.day.eq(1), 0.0, 1.0)
    frame.loc[frame.user_id.str.endswith("0000"), "den"] = 1.0
    stream = Eventstream(
        frame,
        {"segment_cols": ["arm"], "custom_cols": ["num", "den"]},
        preprocess=False,
    )
    draws = stream.get_ab_bootstrap(
        **args(assignment, True),
        period="D",
        contrast={"change": (["2026-01-02"], ["2026-01-01"])},
    )
    assert not draws.loc[draws.contrast.eq("change")].empty
    # Undefined denominator draws remain unavailable; they are not a pairing error.


def test_auto_resamples_reject_extreme_tail_before_allocation(monkeypatch):
    stream, assignment = stream_for(np.arange(40.0), np.arange(40.0) + 1)
    options = args(assignment)
    options.update(
        n_bootstrap="auto", ci_level=np.nextafter(1.0, 0.0), test="bootstrap"
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("oversized request reached allocation")

    monkeypatch.setattr(bs, "bootstrap_paths", forbidden)
    with pytest.raises(ValueError, match="[Rr]esource limit"):
        stream.get_ab_test(**options)


def test_raw_resamples_reject_explicit_excess_before_allocation():
    with pytest.raises(ValueError, match="[Rr]esource limit"):
        bs.bootstrap_paths(np.ones(4), None, np.array([0, 0, 1, 1]), n_bootstrap=10**20)


def test_descriptive_default_does_not_resolve_unused_auto_draws(monkeypatch):
    stream, assignment = stream_for(np.arange(40.0), np.arange(40.0) + 1)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("descriptive default resolved unused draws")

    monkeypatch.setattr(ABTest, "resolved_n_bootstrap", forbidden)
    options = args(assignment)
    options.update(n_bootstrap="auto", ci_level=np.nextafter(1.0, 0.0))
    row = stream.get_ab_test(**options).iloc[0]
    assert row.test == "descriptive"
    assert row.delta == pytest.approx(1.0)


def test_bonferroni_percentile_tails_withheld_when_draws_do_not_resolve_them():
    stream, assignment = stream_for(np.arange(40.0), np.arange(40.0) + 1)
    options = args(assignment)
    options.update(
        metrics=[{**metric(), "name": f"outcome{i}"} for i in range(10)],
        n_bootstrap=1000,
        test="bootstrap",
        correction="bonferroni",
    )
    table = stream.get_ab_test(**options)
    assert np.isfinite(table.delta_ci_low).all()
    assert table.delta_ci_low_adj.isna().all()
    assert table.delta_ci_high_adj.isna().all()
    assert (table.delta_ci_adjusted_status == "insufficient_tail_resamples").all()
    assert (
        table.delta_ci_adjusted_basis == "path_bootstrap_percentile_approximate"
    ).all()
    options["n_bootstrap"] = 4000
    resolved = stream.get_ab_test(**options)
    assert np.isfinite(resolved.delta_ci_low_adj).all()
    assert (resolved.delta_ci_adjusted_status == "ok").all()
