"""Allocation checks use a declared design, including empty planned categories."""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


def _fixture(counts, group_sizes=None):
    rows, assignment = [], []
    for arm, n in counts.items():
        for i in range(n):
            user = f"{arm}_{i}"
            group = (
                "large"
                if group_sizes is None or i < group_sizes.get(arm, 0)
                else "small"
            )
            day = pd.Timestamp("2026-01-01") + pd.Timedelta(days=int(group == "small"))
            rows.append((user, "visit", day + pd.Timedelta(hours=1), arm))
            assignment.append((user, arm, day, group))
    stream = Eventstream(
        pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"]),
        {"path_cols": ["user_id"], "segment_cols": ["arm"]},
    )
    table = pd.DataFrame(
        assignment, columns=["user_id", "arm", "assigned_at", "stratum"]
    ).set_index("user_id")
    return stream, table


def test_planned_unobserved_arm_is_zero_not_a_label_error():
    stream, table = _fixture({"control": 100})
    result = stream.get_sample_ratio(
        assignment=table, expected_shares={"control": 0.5, "treatment": 0.5}
    )
    assert result["levels"]["paths"].tolist() == [100, 0]
    assert result["p_value"] == pytest.approx(2.0**-99)
    assert result["mismatch"]
    assert result["p_value_method"] == "exact_binomial_two_sided"
    assert result["sampling_assumption"] == "independent_bernoulli"
    assert result["inference_status"] == "ok"


def test_sparse_two_arm_check_uses_exact_design_probability():
    stream, table = _fixture({"treatment": 2, "control": 8})
    result = stream.get_sample_ratio(
        assignment=table, expected_shares={"treatment": 0.01, "control": 0.99}
    )
    # P(Binomial(10,.01)>=2), also the probability-ordered two-sided tail here.
    expected = sum(stats.binom.pmf(k, 10, 0.01) for k in range(2, 11))
    assert result["p_value"] == pytest.approx(expected)
    assert result["p_value"] > 0.001 and not result["mismatch"]
    assert result["expected_count_min"] == pytest.approx(0.1)
    assert result["inference_status"] == "ok"


def test_multiarm_sparse_check_is_explicitly_unavailable():
    stream, table = _fixture({"a": 1, "b": 1, "c": 1})
    result = stream.get_sample_ratio(assignment=table)
    assert np.isnan(result["p_value"])
    assert result["chi2"] == 0
    assert result["inference_status"] == "insufficient_expected_counts"
    assert result["support_warning"] == "insufficient_expected_counts"
    assert result["p_value_method"] == "pearson_chi_square_asymptotic"
    assert result["sampling_assumption"] == "independent_multinomial"
    assert result["mismatch"] is None
    assert "unavailable" in result["reading"] and "no mismatch" not in result["reading"]


def test_multiarm_adequate_expected_counts_keep_pearson():
    stream, table = _fixture({"a": 20, "b": 30, "c": 40})
    result = stream.get_sample_ratio(assignment=table)
    expected = stats.chisquare([20, 30, 40])
    assert result["chi2"] == pytest.approx(expected.statistic)
    assert result["p_value"] == pytest.approx(expected.pvalue)
    assert result["inference_status"] == "ok"
    assert result["expected_count_min"] == 30


@pytest.mark.parametrize("split", ["strata", "periods"])
def test_sparse_split_is_unavailable_and_stays_in_planned_holm_family(split):
    stream, table = _fixture({"a": 16, "b": 8, "c": 8}, {"a": 15, "b": 7, "c": 7})
    kwargs = {"strata_col": "stratum"} if split == "strata" else {"period": "D"}
    result = stream.get_sample_ratio(assignment=table, **kwargs)
    rows = result[split].set_index("stratum" if split == "strata" else "period")
    large = rows.loc["large" if split == "strata" else "2026-01-01"]
    small = rows.loc["small" if split == "strata" else "2026-01-02"]
    assert large.p_value_adjusted == pytest.approx(min(1, 2 * large.p_value))
    assert np.isnan(small.p_value) and np.isnan(small.p_value_adjusted)
    assert small.inference_status == "insufficient_expected_counts"
    assert small.mismatch is None
    assert large.p_value_method == "pearson_chi_square_asymptotic"
    assert "unavailable" in result["reading"]


@pytest.mark.parametrize("split", ["strata", "periods"])
def test_two_arm_split_keeps_an_absent_arm_and_exact_p(split):
    stream, table = _fixture({"a": 10, "b": 10}, {"a": 10, "b": 0})
    kwargs = {"strata_col": "stratum"} if split == "strata" else {"period": "D"}
    result = stream.get_sample_ratio(assignment=table, **kwargs)
    for row in result[split].itertuples():
        assert row.p_value == pytest.approx(2.0**-9)
        assert row.p_value_adjusted == pytest.approx(2.0**-8)
        assert row.p_value_method == "exact_binomial_two_sided"
        assert row.sampling_assumption == "independent_bernoulli"
        assert row.inference_status == "ok"


def test_expected_share_aliases_cannot_duplicate_a_planned_arm():
    stream, table = _fixture({1: 10, 2: 10})
    with pytest.raises(InvalidParameterError, match="distinct"):
        stream.get_sample_ratio(assignment=table, expected_shares={1: 0.5, "1": 0.5})


def test_no_counted_assignments_are_unavailable_instead_of_passing():
    stream, table = _fixture({"outside": 10})
    result = stream.get_sample_ratio(
        assignment=table, expected_shares={"a": 0.5, "b": 0.5}, strata_col="stratum"
    )
    assert result["mismatch"] is None
    assert result["inference_status"] == "no_data"
    assert result["unassigned_paths"] == 10
    assert "check unavailable" in result["reading"]
    assert result["strata"].empty


def test_assignment_conflict_still_alarms_when_sparse_test_is_unavailable():
    stream, table = _fixture({"a": 1, "b": 1, "c": 1})
    frame = stream.to_dataframe()
    extra = frame.iloc[:1].copy()
    extra["arm"] = "b"
    extra["timestamp"] += pd.Timedelta(hours=1)
    changed = Eventstream(
        pd.concat([frame, extra]), {"path_cols": ["user_id"], "segment_cols": ["arm"]}
    )
    result = changed.get_sample_ratio("arm", assignment=table)
    assert result["inference_status"] == "insufficient_expected_counts"
    assert result["mismatch"] is True and result["multi_valued_paths"] == 1
    assert "check unavailable" in result["reading"] and "mismatch:" in result["reading"]
