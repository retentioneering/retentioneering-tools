import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    df = pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-01 00:01:00", "US"],
            ["user_1", "C", "2020-01-01 00:02:00", "US"],
            ["user_2", "A", "2020-01-01 00:00:00", "US"],
            ["user_2", "B", "2020-01-01 00:01:00", "US"],
            ["user_3", "A", "2020-01-01 00:00:00", "UK"],
            ["user_4", "A", "2020-01-01 00:00:00", "UK"],
            ["user_4", "B", "2020-01-01 00:01:00", "UK"],
        ],
        columns=["user_id", "event", "timestamp", "country"],
    )
    return df


SCHEMA = {"segment_cols": ["country"]}


def _make_stream() -> Eventstream:
    return Eventstream(get_df(), SCHEMA)


def _sampled_users(stream: Eventstream) -> list[str]:
    return sorted(stream.df["user_id"].astype(str).unique().tolist())


class TestSamplePaths:
    def test__frac_samples_expected_share_of_paths(self) -> None:
        res = _make_stream().sample_paths(frac=0.5, random_state=42)

        users = _sampled_users(res)
        assert len(users) == 2  # 4 paths total, frac=0.5

        # Whole paths are kept: every event of a sampled path survives
        original = get_df()
        for user in users:
            expected_events = original[original["user_id"] == user]["event"].tolist()
            actual_events = res.df[res.df["user_id"].astype(str) == user][
                "event"
            ].tolist()
            assert actual_events == expected_events

    def test__n_samples_expected_number_of_paths(self) -> None:
        res = _make_stream().sample_paths(n=3, random_state=42)
        assert len(_sampled_users(res)) == 3

    def test__random_state_is_reproducible(self) -> None:
        res1 = _make_stream().sample_paths(frac=0.5, random_state=42)
        res2 = _make_stream().sample_paths(frac=0.5, random_state=42)
        assert res1.equals(res2)

        res3 = _make_stream().sample_paths(n=2, random_state=7)
        res4 = _make_stream().sample_paths(n=2, random_state=7)
        assert res3.equals(res4)

    def test__frac_1_returns_stream_unchanged(self) -> None:
        res = _make_stream().sample_paths(frac=1.0, random_state=42)
        expected = Eventstream(get_df(), SCHEMA)
        assert res.equals(expected)

    def test__n_1_keeps_single_whole_path(self) -> None:
        res = _make_stream().sample_paths(n=1, random_state=42)

        users = _sampled_users(res)
        assert len(users) == 1

        original = get_df()
        expected_events = original[original["user_id"] == users[0]]["event"].tolist()
        assert res.df["event"].astype(str).tolist() == expected_events

    def test__n_too_large_returns_all_paths(self) -> None:
        res = _make_stream().sample_paths(n=10, random_state=42)
        expected = Eventstream(get_df(), SCHEMA)
        assert res.equals(expected)

    def test__n_and_frac_together_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            _make_stream().sample_paths(n=2, frac=0.5)

    def test__neither_n_nor_frac_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            _make_stream().sample_paths()

    def test__frac_out_of_range_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            _make_stream().sample_paths(frac=1.5)
