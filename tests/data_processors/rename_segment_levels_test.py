import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    return pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00", "north"],
            ["user_1", "B", "2020-01-02 00:00:00", "north"],
            ["user_2", "A", "2020-01-01 00:00:00", "south"],
            ["user_2", "B", "2020-01-02 00:00:00", "south"],
        ],
        columns=["user_id", "event", "timestamp", "region"],
    )


def get_stream():
    return Eventstream(get_df(), {"segment_cols": ["region"]})


class TestRenameSegmentLevels:
    def test__basic_rename(self) -> None:
        stream = get_stream()

        res = stream.rename_segment_levels("region", {"north": "North America"})

        assert set(res.df["region"].unique().tolist()) == {"North America", "south"}

    def test__multiple_renames(self) -> None:
        stream = get_stream()

        res = stream.rename_segment_levels("region", {"north": "X", "south": "Y"})

        assert set(res.df["region"].unique().tolist()) == {"X", "Y"}

    def test__rename_to_existing_merges_values(self) -> None:
        """Renaming 'north' to 'south' merges it with existing 'south' values."""
        stream = get_stream()

        res = stream.rename_segment_levels("region", {"north": "south"})

        assert set(res.df["region"].cat.categories.tolist()) == {"south"}
        assert (res.df["region"] == "south").all()

    def test__chains_after_add_clusters(self) -> None:
        """rename_segment_levels composes cleanly after add_clusters."""
        df = pd.DataFrame(
            [
                ["user_1", "login", "2020-01-01 00:00:00"],
                ["user_1", "view", "2020-01-01 00:01:00"],
                ["user_2", "login", "2020-01-01 00:00:00"],
                ["user_2", "view", "2020-01-01 00:01:00"],
                ["user_2", "view", "2020-01-01 00:02:00"],
                ["user_2", "view", "2020-01-01 00:03:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.add_clusters(
            name="cluster",
            features=[{"metric": "length"}],
            method="kmeans",
            n_clusters=2,
        ).rename_segment_levels("cluster", {"cluster_0": "short", "cluster_1": "long"})

        assert "cluster" in res.schema.segment_cols
        renamed = set(res.df["cluster"].unique().tolist())
        assert renamed <= {"short", "long"}
        assert not any(c.startswith("cluster_") for c in renamed)

    def test__unknown_segment_col_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            get_stream().rename_segment_levels("unknown_col", {"north": "X"})

    def test__unknown_value_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            get_stream().rename_segment_levels("region", {"UNKNOWN": "X"})

    def test__empty_mapping_is_noop(self) -> None:
        stream = get_stream()

        res = stream.rename_segment_levels("region", {})

        assert res.equals(stream)

    def test__preserves_categorical_dtype(self) -> None:
        stream = get_stream()

        res = stream.rename_segment_levels("region", {"north": "X"})

        assert str(res.df["region"].dtype) == "category"
        assert not res.df["region"].cat.ordered

    def test__unused_categories_removed_after_rename(self) -> None:
        stream = get_stream()

        res = stream.rename_segment_levels("region", {"north": "X"})

        assert "north" not in res.df["region"].cat.categories.tolist()
        assert "X" in res.df["region"].cat.categories.tolist()

    def test__invalid_mapping_type_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            get_stream().rename_segment_levels("region", ["north", "X"])

    def test__non_string_values_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            get_stream().rename_segment_levels("region", {"north": 123})
