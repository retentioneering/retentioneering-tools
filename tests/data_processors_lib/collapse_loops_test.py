from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import CollapseLoops, CollapseLoopsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestCollapseLoops(ApplyTestBase):

    _Processor = CollapseLoops
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_collapse_loops_apply__suffix_count__agg_min(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                timestamp_aggregation_type="min",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00", False],
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00", False],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_apply__suffix_count__agg_max(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                timestamp_aggregation_type="max",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00", False],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_apply__suffix_loop__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                timestamp_aggregation_type="mean",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:02:00", True],
                [1, "event1", "raw", "2022-01-01 00:03:00", True],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30", False],
                [1, "event1", "raw", "2022-01-01 00:04:00", True],
                [1, "event1", "raw", "2022-01-01 00:05:00", True],
                [2, "event1", "raw", "2022-01-02 00:00:00", True],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5", False],
                [2, "event1", "raw", "2022-01-02 00:00:05", True],
            ],
            columns=["user_id", "event", "event_type", "timestamp", "_deleted"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestCollapseLoopsGraph(GraphTestBase):
    _Processor = CollapseLoops
    _source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event1", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:00:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_collapse_loops_graph__suffix_count__agg_min(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                timestamp_aggregation_type="min",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="count",
                timestamp_aggregation_type="max",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix="loop",
                timestamp_aggregation_type="mean",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        actual = self._apply(
            CollapseLoopsParams(
                suffix=None,
                timestamp_aggregation_type="mean",
            )
        )
        expected = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestCollapseLoopsHelper:
    def test_collapse_loops_graph__suffix_count__agg_min(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        params = {"suffix": "count", "timestamp_aggregation_type": "min"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_min = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:02:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:00"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_min).shape == (0, 0)

    def test_collapse_loops_graph__suffix_count__agg_max(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        params = {"suffix": "count", "timestamp_aggregation_type": "max"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_false_max = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop_4", "group_alias", "2022-01-01 00:05:00"],
                [2, "event1_loop_2", "group_alias", "2022-01-02 00:00:05"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_false_max).shape == (0, 0)

    def test_collapse_loops_graph__suffix_loop__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        params = {"suffix": "loop", "timestamp_aggregation_type": "mean"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1_loop", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1_loop", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)

    def test_collapse_loops_graph__suffix_none__agg_mean(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event1", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event1", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:00:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        source = Eventstream(source_df)

        params = {"suffix": None, "timestamp_aggregation_type": "mean"}

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]

        res = source.collapse_loops(**params).to_dataframe()[correct_result_columns].reset_index(drop=True)
        correct_result_true_mean = pd.DataFrame(
            [
                [1, "event1", "raw", "2022-01-01 00:01:00"],
                [1, "event2", "raw", "2022-01-01 00:01:02"],
                [1, "event1", "group_alias", "2022-01-01 00:03:30"],
                [2, "event1", "group_alias", "2022-01-02 00:00:02.5"],
                [2, "event2", "raw", "2022-01-02 00:00:05"],
            ],
            columns=correct_result_columns,
        )

        assert res.compare(correct_result_true_mean).shape == (0, 0)
