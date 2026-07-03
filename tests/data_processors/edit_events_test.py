import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    return pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00"],
            ["user_1", "B", "2020-01-02 00:00:00"],
            ["user_1", "C", "2020-01-03 00:00:00"],
            ["user_2", "A", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-02 00:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )


class TestEditEvents:
    def test__delete_only(self) -> None:
        stream = Eventstream(get_df())

        res = stream.edit_events(delete=["B"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "A", "2020-01-01 00:00:00"],
                    ["user_1", "C", "2020-01-03 00:00:00"],
                    ["user_2", "A", "2020-01-01 00:00:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )

        assert res.equals(expected)

    def test__rename_only(self) -> None:
        stream = Eventstream(get_df())

        res = stream.edit_events(rename={"A": "X"})

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "X", "2020-01-01 00:00:00"],
                    ["user_1", "B", "2020-01-02 00:00:00"],
                    ["user_1", "C", "2020-01-03 00:00:00"],
                    ["user_2", "X", "2020-01-01 00:00:00"],
                    ["user_2", "B", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )

        assert res.equals(expected)

    def test__delete_then_rename(self) -> None:
        """Delete is applied first, rename is applied to the remaining events."""
        stream = Eventstream(get_df())

        res = stream.edit_events(delete=["A"], rename={"B": "X"})

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "X", "2020-01-02 00:00:00"],
                    ["user_1", "C", "2020-01-03 00:00:00"],
                    ["user_2", "X", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )

        assert res.equals(expected)

    def test__delete_multiple(self) -> None:
        stream = Eventstream(get_df())

        res = stream.edit_events(delete=["A", "C"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "B", "2020-01-02 00:00:00"],
                    ["user_2", "B", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )

        assert res.equals(expected)

    def test__rename_multiple(self) -> None:
        stream = Eventstream(get_df())

        res = stream.edit_events(rename={"A": "X", "B": "Y"})

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "X", "2020-01-01 00:00:00"],
                    ["user_1", "Y", "2020-01-02 00:00:00"],
                    ["user_1", "C", "2020-01-03 00:00:00"],
                    ["user_2", "X", "2020-01-01 00:00:00"],
                    ["user_2", "Y", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
        )

        assert res.equals(expected)

    def test__no_args_is_noop(self) -> None:
        stream = Eventstream(get_df())

        res = stream.edit_events()

        assert res.equals(stream)

    def test__overlap_delete_and_rename_raises(self) -> None:
        """An event in both delete and rename is an error."""
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).edit_events(delete=["A"], rename={"A": "X"})

    def test__unknown_delete_event_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).edit_events(delete=["UNKNOWN"])

    def test__unknown_rename_key_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).edit_events(rename={"UNKNOWN": "X"})

    def test__invalid_rename_type_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).edit_events(rename=["A", "X"])

    def test__invalid_delete_type_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).edit_events(delete={"A": True})
