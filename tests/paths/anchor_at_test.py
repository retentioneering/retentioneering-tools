"""An anchor spec's `at` is range-checked the same way at either end.

Kept in its own file rather than appended to `anchors_test.py` only to stay out
of the way of the open PRs editing that one.
"""

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream

# "a->b->.*->c" has three literal tokens; the gap is not a position, so the
# valid `at` range is 0..2 and -3..-1.
PATTERN = "a->b->.*->c"


def _stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "a", "2020-01-01 00:00:00"],
            ["user_1", "b", "2020-01-01 00:01:00"],
            ["user_1", "x", "2020-01-01 00:02:00"],
            ["user_1", "c", "2020-01-01 00:03:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return Eventstream(df, {"event_col": "event"})


def _truncate(at):
    return _stream().truncate_paths({"pattern": PATTERN, "at": at}, "path_end")


class TestAnchorAtRange:
    @pytest.mark.parametrize("at", [3, 7, -4, -9])
    def test_out_of_range_reports_the_pattern_either_way(self, at) -> None:
        # A negative overrun used to surface as the internal
        # "ValueError: max() iterable argument is empty" instead.
        with pytest.raises(IndexError) as exc:
            _truncate(at)
        message = str(exc.value)
        assert f"anchor ordinal {at} is out of range" in message
        assert "3 literal token(s)" in message
        assert "['a', 'b', 'c']" in message

    @pytest.mark.parametrize("at", [0, 1, 2, -1, -2, -3, "start", "end"])
    def test_in_range_values_still_resolve(self, at) -> None:
        assert not _truncate(at).df.empty

    def test_the_two_ends_agree_on_the_same_token(self) -> None:
        # at=0 and at=-3 name the same token, so they must cut identically.
        assert _truncate(0).df.equals(_truncate(-3).df)
        assert _truncate(2).df.equals(_truncate(-1).df)

    def test_range_is_checked_before_any_query_runs(self) -> None:
        # `parse_spec` validates `at` eagerly, so an empty eventstream — which
        # can match nothing — still reports the bad `at` rather than a miss.
        empty = Eventstream(
            pd.DataFrame(
                [["user_1", "zzz", "2020-01-01 00:00:00"]],
                columns=["user_id", "event", "timestamp"],
            ),
            {"event_col": "event"},
        )
        with pytest.raises(IndexError):
            empty.truncate_paths({"pattern": "a->b->c", "at": -4}, "path_end")
