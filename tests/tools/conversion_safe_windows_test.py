"""Keep end selection and whole-pattern eligibility local to each start window."""

import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.paths import anchors


def stream(labels, n_users=1, ties=False):
    return Eventstream(
        pd.DataFrame(
            [
                (
                    str(u),
                    e,
                    pd.Timestamp("2026-01-01")
                    + pd.Timedelta(seconds=j // 2 if ties else j),
                )
                for u in range(n_users)
                for j, e in enumerate(labels)
            ],
            columns=["user_id", "event", "timestamp"],
        )
    )


@pytest.mark.parametrize("within", [1, "1s"])
@pytest.mark.parametrize("end_occurrence", ["first", "last", "all"])
def test_later_start_has_its_own_end_selection(within, end_occurrence):
    result = (
        stream("AXBAB")
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            {"pattern": "B", "occurrence": end_occurrence},
            within=within,
        )
        .iloc[0]
    )
    assert result.paths_with_start == result.converted == 1


@pytest.mark.parametrize("end_occurrence", ["first", "last", "all"])
def test_pattern_lead_in_cannot_come_from_another_start_window(end_occurrence):
    # B belongs after the first A; C is near the second A. Neither complete
    # B->C pattern lies wholly after a start and within two events of it.
    result = (
        stream("ABAXC")
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            {"pattern": "B->.*->C", "occurrence": end_occurrence},
            within=2,
        )
        .iloc[0]
    )
    assert result.converted == 0


def test_last_end_policy_is_not_replaced_with_any_end():
    # An early B is in range, but each start's last B is too far away.
    result = (
        stream("ABAXBXXB")
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            {"pattern": "B", "occurrence": "last"},
            within=1,
        )
        .iloc[0]
    )
    assert result.converted == 0


@pytest.mark.parametrize(
    "end",
    [
        {"pattern": "B", "offset": 1},
        {"pattern": "B", "offset": "1s"},
        {"pattern": "path_end"},
    ],
)
def test_window_offsets_and_virtual_path_end(end):
    assert (
        stream("AXBAB")
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            end,
            within=2,
        )
        .iloc[0]
        .converted
        == 1
    )


def test_tied_times_keep_strict_event_order():
    assert (
        stream("AXBAB", ties=True)
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            "B",
            within="1s",
        )
        .iloc[0]
        .converted
        == 1
    )


def test_batches_by_occurrence_rank_not_individual_path(monkeypatch):
    original = anchors.resolve_positions
    calls = []

    def counted(*args, **kwargs):
        if kwargs.get("not_before") is not None:
            floor = kwargs["not_before"]
            assert not floor.user_id.duplicated().any()
            calls.append(len(floor))
        return original(*args, **kwargs)

    monkeypatch.setattr(anchors, "resolve_positions", counted)
    result = (
        stream("AXBAB", n_users=30)
        .get_conversion_rate(
            {"pattern": "A", "occurrence": "all"},
            "B",
            within=1,
        )
        .iloc[0]
    )
    assert result.converted == 30
    assert calls == [30, 30]


def test_numeric_occurrence_remains_explicitly_unsupported():
    with pytest.raises(InvalidParameterError, match="occurrence"):
        stream("AXBAB").get_conversion_rate("A", {"pattern": "B", "occurrence": 2})
