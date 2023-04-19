from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from retentioneering.data_processors_lib import (
    LabelCroppedPaths,
    LabelCroppedPathsParams,
)
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestLabelCroppedPaths(ApplyTestBase):
    _Processor = LabelCroppedPaths
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:00:00"],
            [2, "event1", "2022-01-01 00:30:00"],
            [2, "event2", "2022-01-01 00:31:00"],
            [3, "event1", "2022-01-01 01:00:01"],
            [3, "event2", "2022-01-01 01:00:02"],
            [4, "event1", "2022-01-01 02:01:00"],
            [4, "event2", "2022-01-01 02:02:00"],
            [5, "event1", "2022-01-01 03:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_cropped_paths_apply__left_right(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                left_cutoff=(1, "h"),
                right_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "cropped_left", "cropped_left", "2022-01-01 00:00:00"],
                [2, "cropped_left", "cropped_left", "2022-01-01 00:30:00"],
                [4, "cropped_right", "cropped_right", "2022-01-01 02:02:00"],
                [5, "cropped_right", "cropped_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_cropped_paths_apply__left(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                left_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "cropped_left", "cropped_left", "2022-01-01 00:00:00"],
                [2, "cropped_left", "cropped_left", "2022-01-01 00:30:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_cropped_paths_apply__right(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                right_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [4, "cropped_right", "cropped_right", "2022-01-01 02:02:00"],
                [5, "cropped_right", "cropped_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = LabelCroppedPathsParams(left_cutoff=(1, "xxx"))


class TestLabelCroppedPathsGraph(GraphTestBase):
    _Processor = LabelCroppedPaths
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:00:00"],
            [2, "event1", "2022-01-01 00:30:00"],
            [2, "event2", "2022-01-01 00:31:00"],
            [3, "event1", "2022-01-01 01:00:01"],
            [3, "event2", "2022-01-01 01:00:02"],
            [4, "event1", "2022-01-01 02:01:00"],
            [4, "event2", "2022-01-01 02:02:00"],
            [5, "event1", "2022-01-01 03:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_label_cropped_paths_graph__left_right(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                left_cutoff=(1, "h"),
                right_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "cropped_left", "cropped_left", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "cropped_left", "cropped_left", "2022-01-01 00:30:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [4, "cropped_right", "cropped_right", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
                [5, "cropped_right", "cropped_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_cropped_paths_graph__left(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                left_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "cropped_left", "cropped_left", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "cropped_left", "cropped_left", "2022-01-01 00:30:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_label_cropped_paths_graph__right(self):
        actual = self._apply(
            LabelCroppedPathsParams(
                right_cutoff=(1, "h"),
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [4, "cropped_right", "cropped_right", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
                [5, "cropped_right", "cropped_right", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestLabelCroppedPathsHelper:
    def test_label_cropped_paths_graph(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [2, "event1", "2022-01-01 00:30:00"],
                [2, "event2", "2022-01-01 00:31:00"],
                [3, "event1", "2022-01-01 01:00:01"],
                [3, "event2", "2022-01-01 01:00:02"],
                [4, "event1", "2022-01-01 02:01:00"],
                [4, "event2", "2022-01-01 02:02:00"],
                [5, "event1", "2022-01-01 03:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "cropped_left", "cropped_left", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [2, "cropped_left", "cropped_left", "2022-01-01 00:30:00"],
                [2, "event1", "raw", "2022-01-01 00:30:00"],
                [2, "event2", "raw", "2022-01-01 00:31:00"],
                [3, "event1", "raw", "2022-01-01 01:00:01"],
                [3, "event2", "raw", "2022-01-01 01:00:02"],
                [4, "event1", "raw", "2022-01-01 02:01:00"],
                [4, "event2", "raw", "2022-01-01 02:02:00"],
                [4, "cropped_right", "cropped_right", "2022-01-01 02:02:00"],
                [5, "event1", "raw", "2022-01-01 03:00:00"],
                [5, "cropped_right", "cropped_right", "2022-01-01 03:00:00"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(source_df)

        res = stream.label_cropped_paths(left_cutoff=(1, "h"), right_cutoff=(1, "h")).to_dataframe()[
            correct_result_columns
        ]

        assert res.compare(correct_result).shape == (0, 0)
