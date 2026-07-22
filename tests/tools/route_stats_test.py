import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError


def _stream() -> Eventstream:
    # user 1: A B C        (route A→B once)
    # user 2: A B B C      (A→B once, B→B once; strict A→B→C absent)
    # user 3: A C          (no A→B)
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 2, 2, 3, 3],
            "event": ["A", "B", "C", "A", "B", "B", "C", "A", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=9, freq="1min"),
        }
    )
    return Eventstream(df)


class TestRouteStats:
    def test__edge_route(self):
        stats = _stream().route_stats(["A", "B"])

        assert stats["n_paths"] == 3
        assert stats["unique_paths"] == 2
        assert stats["unique_paths_share"] == pytest.approx(2 / 3)
        assert stats["occurrences"] == 2
        assert stats["avg_per_path"] == pytest.approx(2 / 3)
        # both A→B transitions are 1 minute apart
        assert stats["time_median"] == pytest.approx(60.0)
        # A has 3 outgoing transitions (B, B, C), 2 of them to B
        assert stats["proba"] == pytest.approx(2 / 3)

    def test__strict_matching_no_stutter_collapsing(self):
        # user 2 has A B B C — strict A→B→C only matches user 1
        stats = _stream().route_stats(["A", "B", "C"])

        assert stats["unique_paths"] == 1
        assert stats["occurrences"] == 1
        assert stats["time_median"] == pytest.approx(120.0)
        # P(A→B) = 2/3, P(B→C) = 2/3 (B's outgoing: B→C, B→B, B→C)
        assert stats["proba"] == pytest.approx(4 / 9)

    def test__self_loop_route(self):
        stats = _stream().route_stats(["B", "B"])

        assert stats["unique_paths"] == 1
        assert stats["occurrences"] == 1

    def test__overlapping_occurrences_count(self):
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1],
                "event": ["A", "A", "A"],
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="1min"),
            }
        )
        stats = Eventstream(df).route_stats(["A", "A"])

        # A,A,A contains A→A twice (overlapping), same as transition counts
        assert stats["occurrences"] == 2
        assert stats["unique_paths"] == 1

    def test__no_occurrences(self):
        stats = _stream().route_stats(["C", "A"])

        assert stats["unique_paths"] == 0
        assert stats["occurrences"] == 0
        assert stats["time_median"] is None
        assert stats["time_q95"] is None
        assert stats["proba"] == 0.0

    def test__route_with_path_boundaries(self):
        stats = _stream().route_stats(["path_start", "A"])

        assert stats["unique_paths"] == 3
        assert stats["unique_paths_share"] == pytest.approx(1.0)

    def test__route_too_short(self):
        with pytest.raises(InvalidParameterError):
            _stream().route_stats(["A"])

    def test__quote_in_event_name(self):
        df = pd.DataFrame(
            {
                "user_id": [1, 1],
                "event": ["it's a trap", "B"],
                "timestamp": pd.date_range("2024-01-01", periods=2, freq="1min"),
            }
        )
        stats = Eventstream(df).route_stats(["it's a trap", "B"])

        assert stats["unique_paths"] == 1
