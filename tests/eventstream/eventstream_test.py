from __future__ import annotations

import math
import uuid

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from retentioneering.utils import shuffle_df
from tests.eventstream.fixtures.eventstream import (
    test_data_1,
    test_data_join_1,
    test_data_join_2,
    test_data_sampling,
    test_data_with_custom_col_and_type,
    test_df_identical_timestamps,
    test_df_identical_timestamps_corr,
    test_schema_1,
    test_source_dataframe_with_custom_col,
    test_stream_1,
    test_stream_2,
)


class TestEventstream:
    def test_create_eventstream(self, test_stream_1):
        df = test_stream_1.to_dataframe()
        schema = test_stream_1.schema
        columns = df.columns

        assert schema.event_id in columns
        assert schema.event_type in columns
        assert schema.event_name in columns
        assert schema.event_timestamp in columns
        assert schema.user_id in columns

        for [_, event] in df.iterrows():
            assert event[schema.event_type] == "raw"
            assert isinstance(event[schema.event_id], uuid.UUID)

    def test_create_eventstream__dict_raw_data_schema(self, test_source_dataframe_with_custom_col):
        stream = Eventstream(
            raw_data=test_source_dataframe_with_custom_col,
            raw_data_schema={
                "event_timestamp": "event_timestamp",
                "user_id": "user_id",
                "event_name": "action",
                "custom_cols": [{"raw_data_col": "random_col", "custom_col": "random_col"}],
            },
        )
        df = stream.to_dataframe()
        assert "event" in df.columns
        assert "random_col" in df.columns
        try:
            stream.add_start_end_events()
        except Exception as e:
            raise pytest.UsageError(e)

    def test_create_custom_cols(self, test_data_1, test_schema_1):
        custom_cols = ["custom_col_1", "custom_col_2"]
        es = Eventstream(
            raw_data_schema=test_schema_1,
            raw_data=test_data_1,
            schema=EventstreamSchema(custom_cols=custom_cols),
        )
        df = es.to_dataframe()

        assert es.schema.custom_cols == custom_cols

        for custom_col in custom_cols:
            assert custom_col in df.columns

    def test_get_prepared_custom_col(self, test_data_1, test_schema_1):
        custom_cols = ["custom_col_1", "custom_col_2"]

        raw_data = test_data_1.copy()
        raw_data[custom_cols[0]] = "custom_col_value"
        raw_data[custom_cols[1]] = "custom_col_value"

        raw_data_schema = test_schema_1.copy()
        raw_data_schema.custom_cols = [{"custom_col": col, "raw_data_col": col} for col in custom_cols]

        es = Eventstream(
            raw_data_schema=raw_data_schema, raw_data=raw_data, schema=EventstreamSchema(custom_cols=custom_cols)
        )

        schema = es.schema

        df = es.to_dataframe()
        for custom_col in custom_cols:
            assert custom_col in df.columns

        for [i, event] in df.iterrows():
            assert event[schema.custom_cols[0]] == "custom_col_value"
            assert event[schema.custom_cols[1]] == "custom_col_value"

    def test_index_events(self, test_stream_2):
        df = test_stream_2.to_dataframe()
        names: list[str] = [event[test_stream_2.schema.event_name] for [_, event] in df.iterrows()]
        assert names == ["pageview", "click_1", "path_start", "click_2", "absent_user", "path_end"]

    def test_sampling__user_sample_size__float(self, test_data_sampling):
        user_sample_share = 0.8
        es = Eventstream(test_data_sampling)
        es_sampled_1 = Eventstream(test_data_sampling, user_sample_size=user_sample_share)
        df, df_sampled_1 = es.to_dataframe(), es_sampled_1.to_dataframe()
        user_cnt = len(df["user_id"].unique())
        user_cnt_sampled_1 = len(df_sampled_1["user_id"].unique())

        assert math.isclose(user_cnt * user_sample_share, user_cnt_sampled_1, abs_tol=0.51)

    def test_sampling__user_sample_size__int(self, test_data_sampling):
        user_sample_size = 3
        es = Eventstream(test_data_sampling)
        es_sampled_2 = Eventstream(test_data_sampling, user_sample_size=user_sample_size)
        df, df_sampled_2 = es.to_dataframe(), es_sampled_2.to_dataframe()
        user_cnt = len(df["user_id"].unique())
        user_cnt_sampled_2 = len(df_sampled_2["user_id"].unique())

        assert math.isclose(user_sample_size, user_cnt_sampled_2, abs_tol=0.51)

    def test_describe_works(self, test_stream_1):
        try:
            test_stream_1.describe()
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.describe. " + str(e))
        try:
            test_stream_1.describe_events()
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.describe_events. " + str(e))

    def test_describe_works_correctly(self, test_stream_1):
        pass

    def test_hists(self, test_stream_1):
        try:
            test_stream_1.timedelta_hist(show_plot=False)
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.timedelta_hist. " + str(e))
        try:
            test_stream_1.user_lifetime_hist(show_plot=False)
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.user_lifetime_hist. " + str(e))
        try:
            test_stream_1.event_timestamp_hist(show_plot=False)
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.event_timestamp_hist. " + str(e))

    def test_pass_events_order(self, test_df_identical_timestamps, test_df_identical_timestamps_corr):
        stream = Eventstream(
            raw_data_schema=RawDataSchema(
                user_id="user_id", event_name="event", event_type="event_type", event_timestamp="timestamp"
            ),
            raw_data=test_df_identical_timestamps,
            schema=EventstreamSchema(),
            events_order=["path_start", "event2", "event3", "event4", "event5"],
            add_start_end_events=False,
        )

        actual = stream.to_dataframe()
        expected = test_df_identical_timestamps_corr.copy()
        expected.timestamp = pd.to_datetime(expected.timestamp)

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None

    def test_custom_cols(self, test_data_with_custom_col_and_type):
        stream = Eventstream(
            raw_data_schema=RawDataSchema(
                user_id="user_id", event_name="action", event_type="event_type", event_timestamp="event_timestamp"
            ),
            raw_data=test_data_with_custom_col_and_type,
            custom_cols=["random_col"],
            add_start_end_events=False,
        )

        df = stream.to_dataframe()
        random_col = list(df["random_col"])

        assert stream.schema.custom_cols == ["random_col"]
        assert "random_col" in df.columns
        assert len(random_col) == 3

    def test_custom_cols_autodetect(self, test_data_with_custom_col_and_type):
        stream = Eventstream(
            raw_data_schema=RawDataSchema(user_id="user_id", event_name="action", event_timestamp="event_timestamp"),
            schema=EventstreamSchema(event_id="event_id", event_type="event_type"),
            raw_data=test_data_with_custom_col_and_type,
            add_start_end_events=False,
        )

        df = stream.to_dataframe()
        random_col = list(df["random_col"])

        assert stream.schema.custom_cols == ["random_col"]
        assert "random_col" in df.columns
        assert len(random_col) == 3

    def test_schema_as_dict(self, test_data_1, test_schema_1):
        stream = Eventstream(
            raw_data=test_data_1,
            raw_data_schema={"event_name": "name", "event_timestamp": "event_timestamp", "user_id": "user_id"},
            schema={"event_name": "event_name_custom"},
            add_start_end_events=False,
        )

        df = stream.to_dataframe()
        event_name_col = list(df["event_name_custom"])

        assert stream.schema.event_name == "event_name_custom"
        assert len(event_name_col) == 3
        assert event_name_col == ["pageview", "click_1", "click_2"]
