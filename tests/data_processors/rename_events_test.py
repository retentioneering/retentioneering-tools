import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    return pd.DataFrame([
        ["user_1", "A", "2020-01-01 00:00:00"],
        ["user_1", "B", "2020-01-02 00:00:00"],
        ["user_1", "C", "2020-01-03 00:00:00"],
        ["user_2", "A", "2020-01-01 00:00:00"],
        ["user_2", "B", "2020-01-02 00:00:00"],
    ], columns=["user_id", "event", "timestamp"])


class TestRenameEvents:

    def test__basic_rename(self) -> None:
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={"A": "X"})

        expected = Eventstream(pd.DataFrame([
            ["user_1", "X", "2020-01-01 00:00:00"],
            ["user_1", "B", "2020-01-02 00:00:00"],
            ["user_1", "C", "2020-01-03 00:00:00"],
            ["user_2", "X", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-02 00:00:00"],
        ], columns=["user_id", "event", "timestamp"]))

        assert res.equals(expected)

    def test__multiple_renames(self) -> None:
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={"A": "X", "B": "Y"})

        expected = Eventstream(pd.DataFrame([
            ["user_1", "X", "2020-01-01 00:00:00"],
            ["user_1", "Y", "2020-01-02 00:00:00"],
            ["user_1", "C", "2020-01-03 00:00:00"],
            ["user_2", "X", "2020-01-01 00:00:00"],
            ["user_2", "Y", "2020-01-02 00:00:00"],
        ], columns=["user_id", "event", "timestamp"]))

        assert res.equals(expected)

    def test__rename_to_existing_merges_events(self) -> None:
        """Renaming event A to B merges it with existing B events."""
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={"A": "B"})

        result_events = set(res.df["event"].cat.categories.tolist())
        assert result_events == {"B", "C"}

        result_counts = res.df["event"].value_counts().to_dict()
        assert result_counts["B"] == 4
        assert result_counts["C"] == 1

    def test__unknown_key_raises(self) -> None:
        """Keys not present in the event column raise PreprocessingConfigError."""
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).rename_events(mapping={"A": "X", "UNKNOWN": "Y"})

    def test__empty_mapping_is_noop(self) -> None:
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={})

        assert res.equals(stream)

    def test__preserves_categorical_dtype(self) -> None:
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={"A": "X"})

        assert str(res.df["event"].dtype) == "category"
        assert not res.df["event"].cat.ordered

    def test__unused_categories_removed_after_rename(self) -> None:
        """After renaming A→X, category 'A' must not remain in the index."""
        stream = Eventstream(get_df())

        res = stream.rename_events(mapping={"A": "X"})

        assert "A" not in res.df["event"].cat.categories.tolist()
        assert "X" in res.df["event"].cat.categories.tolist()

    def test__invalid_mapping_type_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).rename_events(mapping=["A", "X"])

    def test__non_string_values_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).rename_events(mapping={"A": 123})
