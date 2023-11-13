from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from retentioneering import datasets
from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase
from tests.data_processors_lib.fixtures.split_sessions_corr import (
    basic_corr,
    delimiter_col_corr,
    mark_truncated_true_corr,
    one_delimiter_event_corr,
    two_delimiter_events_corr,
)
from tests.data_processors_lib.fixtures.split_sessions_input import (
    test_df_1,
    test_df_2,
    test_df_3,
    test_df_4,
    test_df_5,
)


class TestSplitSessions(ApplyTestBase):
    _Processor = SplitSessions
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_type="event_type",
        event_timestamp="timestamp",
    )

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            p = SplitSessionsParams(timeout=(1, "xxx"))

    def test_params__check_set_of_params(self):
        with pytest.raises(ValueError):
            stream = datasets.load_simple_shop(add_start_end_events=False)
            stream.split_sessions(delimiter_events=["main", "catalog"], delimiter_col="event")
            stream.split_sessions(delimiter_events=["main", "catalog"], timeout=(30, "m"))
            stream.split_sessions()

    def test_split_session_apply_1_basic(self, test_df_1: pd.DataFrame, basic_corr: pd.DataFrame) -> None:
        stream = Eventstream(test_df_1, add_start_end_events=False)

        dataprocessor = SplitSessions(
            params=SplitSessionsParams(
                timeout=(30, "m"),
                session_col="session_id",
            )
        )

        actual = dataprocessor.apply(df=stream.to_dataframe(copy=True), schema=EventstreamSchema())

        raw_data_schema_new = dict(
            **{
                "event_type": "event_type",
                "event_index": "event_index",
                "custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}],
            }
        )
        actual_stream = Eventstream(
            raw_data=actual,
            raw_data_schema=raw_data_schema_new,
            add_start_end_events=False,
        )

        actual = actual_stream.to_dataframe()

        correct_result = basic_corr
        correct_result_columns = correct_result.columns

        res = actual.sort_values(["user_id", "event_index"])[correct_result_columns].reset_index(drop=True)
        assert res.compare(correct_result).shape == (0, 0)

    def test_split_session_apply_2_mark_truncated_true(
        self, test_df_2: pd.DataFrame, mark_truncated_true_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df_2, add_start_end_events=False)

        dataprocessor = SplitSessions(
            params=SplitSessionsParams(
                timeout=(30, "m"),
                session_col="session_id",
                mark_truncated=True,
            )
        )

        actual = dataprocessor.apply(df=stream.to_dataframe(copy=True), schema=EventstreamSchema())

        raw_data_schema_new = dict(
            **{
                "event_type": "event_type",
                "event_index": "event_index",
                "custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}],
            }
        )
        actual_stream = Eventstream(
            raw_data=actual,
            raw_data_schema=raw_data_schema_new,
            add_start_end_events=False,
        )

        actual = actual_stream.to_dataframe()

        correct_result = mark_truncated_true_corr
        correct_result_columns = correct_result.columns

        res = actual.sort_values(["user_id", "event_index"])[correct_result_columns].reset_index(drop=True)
        assert res.compare(correct_result).shape == (0, 0)

    def test_split_session_apply_3_one_delimiter_event(
        self, test_df_3: pd.DataFrame, one_delimiter_event_corr: pd.DataFrame
    ) -> None:
        dataprocessor = SplitSessions(
            params=SplitSessionsParams(
                delimiter_events=["custom_start"],
            )
        )

        stream = Eventstream(test_df_3, add_start_end_events=False)
        actual = dataprocessor.apply(df=stream.to_dataframe(copy=True), schema=EventstreamSchema())

        actual_stream = Eventstream(
            raw_data=actual,
            raw_data_schema=self._raw_data_schema,
            add_start_end_events=False,
        )

        actual = actual_stream.to_dataframe()
        expected = one_delimiter_event_corr
        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_apply_4_two_delimiter_events(
        self, test_df_4: pd.DataFrame, two_delimiter_events_corr: pd.DataFrame
    ) -> None:
        dataprocessor = SplitSessions(params=SplitSessionsParams(delimiter_events=["custom_start", "custom_end"]))

        stream = Eventstream(test_df_4, add_start_end_events=False)
        actual = dataprocessor.apply(df=stream.to_dataframe(copy=True), schema=EventstreamSchema())

        actual_stream = Eventstream(
            raw_data=actual,
            raw_data_schema=self._raw_data_schema,
            add_start_end_events=False,
        )

        actual = actual_stream.to_dataframe()

        expected = two_delimiter_events_corr
        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_apply_5_delimiter_col(
        self, test_df_5: pd.DataFrame, delimiter_col_corr: pd.DataFrame
    ) -> None:
        dataprocessor = SplitSessions(params=SplitSessionsParams(delimiter_col="session_id"))
        raw_data_schema = {"custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}]}
        stream = Eventstream(test_df_5, raw_data_schema=raw_data_schema, add_start_end_events=False)
        actual = dataprocessor.apply(df=stream.to_dataframe(copy=True), schema=EventstreamSchema())

        raw_data_schema_new = dict(raw_data_schema, **{"event_type": "event_type", "event_index": "event_index"})
        actual_stream = Eventstream(actual, raw_data_schema=raw_data_schema_new, add_start_end_events=False)

        actual = actual_stream.to_dataframe()

        expected = delimiter_col_corr
        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestSplitSessionsGraph(GraphTestBase):
    _Processor = SplitSessions
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_split_sesssion_graph_1_basic(self, test_df_1: pd.DataFrame, basic_corr: pd.DataFrame) -> None:
        actual = self._apply(
            SplitSessionsParams(
                timeout=(30, "m"),
                session_col="session_id",
            ),
            source_df=test_df_1,
        )

        correct_result = basic_corr
        correct_result_columns = correct_result.columns

        res = actual[correct_result_columns].sort_values(["user_id", "timestamp"]).reset_index(drop=True)
        assert res.compare(correct_result).shape == (0, 0)

    def test_split_sesssion_graph_2_mark_truncated_true(
        self, test_df_2: pd.DataFrame, mark_truncated_true_corr: pd.DataFrame
    ) -> None:
        actual = self._apply(
            SplitSessionsParams(
                timeout=(30, "m"),
                session_col="session_id",
                mark_truncated=True,
            ),
            source_df=test_df_2,
        )

        correct_result = mark_truncated_true_corr
        correct_result_columns = correct_result.columns

        res = actual.sort_values(["user_id", "event_index"])[correct_result_columns].reset_index(drop=True)
        assert res.compare(correct_result).shape == (0, 0)

    def test_split_session_graph_3_one_delimiter_event(
        self, test_df_3: pd.DataFrame, one_delimiter_event_corr: pd.DataFrame
    ) -> None:
        actual = self._apply(
            SplitSessionsParams(delimiter_events=["custom_start"]),
            source_df=test_df_3,
        )

        expected = one_delimiter_event_corr

        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_graph_4_two_delimiter_events(
        self, test_df_4: pd.DataFrame, two_delimiter_events_corr: pd.DataFrame
    ) -> None:
        actual = self._apply(
            SplitSessionsParams(delimiter_events=["custom_start", "custom_end"]),
            source_df=test_df_4,
        )

        expected = two_delimiter_events_corr

        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_graph_5_delimiter_col(
        self, test_df_5: pd.DataFrame, delimiter_col_corr: pd.DataFrame
    ) -> None:
        raw_data_schema = {"custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}]}
        actual = self._apply(
            SplitSessionsParams(delimiter_col="session_id"), source_df=test_df_5, raw_data_schema=raw_data_schema
        )

        expected = delimiter_col_corr

        actual = actual.sort_values(["user_id", "event_index"])[expected.columns].reset_index(drop=True)

        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestSplitSessionsHelper:
    def test_split_sesssion_helper_1_basic(self, test_df_1: pd.DataFrame, basic_corr: pd.DataFrame) -> None:
        correct_result = basic_corr
        correct_result_columns = correct_result.columns

        stream = Eventstream(test_df_1, add_start_end_events=False)

        res = (
            stream.split_sessions(timeout=(30, "m"), session_col="session_id")
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)

    def test_split_sesssion_helper_2_mark_truncated_true(
        self, test_df_2: pd.DataFrame, mark_truncated_true_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df_2, add_start_end_events=False)
        correct_result = mark_truncated_true_corr
        correct_result_columns = correct_result.columns

        res = (
            stream.split_sessions(timeout=(30, "m"), session_col="session_id", mark_truncated=True)
            .to_dataframe()[correct_result_columns]
            .sort_values(["user_id", "timestamp"])
            .reset_index(drop=True)
        )

        assert res.compare(correct_result).shape == (0, 0)

    def test_split_session_helper_3_one_delimiter_event(
        self, test_df_3: pd.DataFrame, one_delimiter_event_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df_3, add_start_end_events=False)

        expected = one_delimiter_event_corr

        actual = (
            stream.split_sessions(delimiter_events=["custom_start"])
            .to_dataframe()
            .sort_values(["user_id", "event_index"])[expected.columns]
            .reset_index(drop=True)
        )

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_helper_4_two_delimiter_events(
        self, test_df_4: pd.DataFrame, two_delimiter_events_corr: pd.DataFrame
    ) -> None:
        stream = Eventstream(test_df_4, add_start_end_events=False)

        expected = two_delimiter_events_corr

        actual = (
            stream.split_sessions(delimiter_events=["custom_start", "custom_end"])
            .to_dataframe()
            .sort_values(["user_id", "event_index"])
            .reset_index(drop=True)
        )

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_split_session_apply_5_delimiter_col(
        self, test_df_5: pd.DataFrame, delimiter_col_corr: pd.DataFrame
    ) -> None:
        raw_data_schema = {"custom_cols": [{"raw_data_col": "session_id", "custom_col": "session_id"}]}
        stream = Eventstream(test_df_5, raw_data_schema=raw_data_schema, add_start_end_events=False)

        expected = delimiter_col_corr

        actual = (
            stream.split_sessions(delimiter_col="session_id")
            .to_dataframe()
            .sort_values(["user_id", "event_index"])
            .reset_index(drop=True)
        )

        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_params_model__incorrect_datetime_unit(self):
        with pytest.raises(ValidationError):
            source_df = pd.DataFrame(
                [
                    [111, "event1", "2022-01-01 00:00:00"],
                    [111, "event2", "2022-01-01 00:01:00"],
                    [111, "event3", "2022-01-01 00:33:00"],
                    [111, "event4", "2022-01-01 00:34:00"],
                    [222, "event1", "2022-01-01 00:30:00"],
                    [222, "event2", "2022-01-01 00:31:00"],
                    [222, "event3", "2022-01-01 01:01:00"],
                    [333, "event1", "2022-01-01 01:00:00"],
                    [333, "event2", "2022-01-01 01:01:00"],
                    [333, "event3", "2022-01-01 01:32:00"],
                    [333, "event4", "2022-01-01 01:33:00"],
                ],
                columns=["user_id", "event", "timestamp"],
            )
            correct_result_columns = ["user_id", "event", "event_type", "timestamp", "session_id"]

            stream = Eventstream(source_df, add_start_end_events=False)

            res = (
                stream.split_sessions(timeout=(30, "xxx"), session_col="session_id", mark_truncated=True)
                .to_dataframe()[correct_result_columns]
                .sort_values(["user_id", "timestamp"])
                .reset_index(drop=True)
            )

    def test_params_model__two_delimiting_params(self) -> None:
        with pytest.raises(ValueError):
            stream = Eventstream(pd.DataFrame(columns=["user_id", "event", "timestamp"]))
            res = stream.split_sessions(timeout=(30, "m"), delimiter_events=["pageview"])

    def test_params_model__no_delimiting_params(self) -> None:
        with pytest.raises(ValueError):
            stream = Eventstream(pd.DataFrame(columns=["user_id", "event", "timestamp"]))
            res = stream.split_sessions()
