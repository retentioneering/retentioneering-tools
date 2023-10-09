from __future__ import annotations

import math
import uuid

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import DELETE_COL_NAME, Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from retentioneering.utils import shuffle_df
from tests.eventstream.fixtures.eventstream import (
    test_data_1,
    test_data_join_1,
    test_data_join_2,
    test_data_sampling,
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

    def test_create_relation(self, test_stream_1):
        # shuffle data
        df = test_stream_1.to_dataframe()
        parent_df = shuffle_df(df)
        parent_df["ref"] = parent_df[test_stream_1.schema.event_id]

        related_es = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name=test_stream_1.schema.event_name,
                event_type=test_stream_1.schema.event_type,
                event_timestamp=test_stream_1.schema.event_timestamp,
                user_id=test_stream_1.schema.user_id,
            ),
            raw_data=parent_df,
            schema=EventstreamSchema(event_name="name"),
            relations=[{"eventstream": test_stream_1, "raw_col": "ref"}],
        )

        related_df = related_es.to_dataframe()

        for [i, event] in related_df.iterrows():
            query: pd.Series[bool] = parent_df[test_stream_1.schema.event_id] == event["ref_0"]
            related_event = parent_df[query].iloc[0]
            assert related_event[test_stream_1.schema.event_id] == event["ref_0"]
            assert related_event[test_stream_1.schema.event_name] == event[related_es.schema.event_name]
            assert related_event[test_stream_1.schema.event_timestamp] == event[related_es.schema.event_timestamp]
            assert related_event[test_stream_1.schema.user_id] == event[related_es.schema.user_id]

    def test_join_eventstream(self, test_data_join_1, test_data_join_2):
        schema = EventstreamSchema(
            event_id="id",
            event_name="event_name",
            event_timestamp="event_timestamp",
            user_id="user_id",
            event_index="index",
            event_type="type",
            custom_cols=[],
        )

        source_schema = RawDataSchema(event_name="event_name", event_timestamp="event_timestamp", user_id="user_id")

        child_schema = RawDataSchema(
            event_name="name", event_timestamp="event_timestamp", user_id="user_id", event_type="type"
        )

        source = Eventstream(raw_data_schema=source_schema, raw_data=test_data_join_1, schema=schema)

        source_events_df = source.to_dataframe()

        join_df = test_data_join_2
        join_df["ref_id"] = [source_events_df.iloc[1]["id"], source_events_df.iloc[3]["id"], uuid.uuid4(), None]

        child = Eventstream(
            raw_data_schema=child_schema,
            raw_data=join_df,
            schema=schema,
            relations=[{"eventstream": source, "raw_col": "ref_id"}],
        )

        child_events_df = child.to_dataframe()

        source._join_eventstream(child)
        result_df = source.to_dataframe()

        names: list[str] = result_df[schema.event_name].to_list()
        timestamps: list[int] = result_df[schema.event_timestamp].to_list()
        user_ids: list[str] = result_df[schema.user_id].astype(int).astype(str).to_list()
        types: list[str] = result_df[schema.event_type].to_list()

        assert names == ["pageview", "add_to_cart", "pageview", "add_to_cart", "add_to_cart123"]

        assert types == ["raw", "synthetic", "raw", "synthetic", "synthetic"]

        expected_timestamps: list[int] = [
            source_events_df.iloc[0][schema.event_timestamp],
            child_events_df.iloc[0][schema.event_timestamp],
            source_events_df.iloc[2][schema.event_timestamp],
            child_events_df.iloc[1][schema.event_timestamp],
            child_events_df.iloc[3][schema.event_timestamp],
        ]

        assert timestamps == expected_timestamps
        assert user_ids == ["1", "1", "1", "1", "2"]

        result_with_deleted = source.to_dataframe(show_deleted=True)
        deleted_events = result_with_deleted[result_with_deleted[DELETE_COL_NAME] == True]
        deleted_events_names: list[str] = deleted_events[schema.event_name].to_list()

        assert deleted_events_names == []

    def test_soft_delete(self, test_stream_1):
        df = test_stream_1.to_dataframe()

        test_stream_1._soft_delete(events=df[df[test_stream_1.schema.event_name] == "pageview"])

        after_delete = test_stream_1.to_dataframe()
        after_delete_all = test_stream_1.to_dataframe(show_deleted=True)

        event_names: list[str] = after_delete[test_stream_1.schema.event_name].to_list()
        with_deleted_event_names: list[str] = after_delete_all[test_stream_1.schema.event_name].to_list()

        assert event_names == ["click_1", "click_2"]

        assert with_deleted_event_names == ["pageview", "click_1", "click_2"]

        test_stream_1._soft_delete(events=after_delete[after_delete[test_stream_1.schema.event_name] == "click_1"])

        after_delete = test_stream_1.to_dataframe()
        after_delete_all = test_stream_1.to_dataframe(show_deleted=True)

        event_names: list[str] = after_delete[test_stream_1.schema.event_name].to_list()
        with_deleted_event_names: list[str] = after_delete_all[test_stream_1.schema.event_name].to_list()

        assert event_names == ["click_2"]

        assert with_deleted_event_names == ["pageview", "click_1", "click_2"]

    def test_delete_events_by_join(self, test_data_1, test_schema_1):
        source = Eventstream(raw_data_schema=test_schema_1, raw_data=test_data_1, schema=EventstreamSchema())
        df = source.to_dataframe()

        source._soft_delete(events=df[df[source.schema.event_name] == "pageview"])

        related_cols: list[str] = test_data_1.columns.to_list()
        related_cols.append("ref_id")
        related_df = pd.DataFrame(test_data_1.copy(), columns=related_cols)
        related_df = related_df[(related_df["name"] == "pageview") | (related_df["name"] == "click_1")]
        related_df.reset_index(inplace=True, drop=True)
        related_df.at[0, "ref_id"] = df.iloc[0][source.schema.event_id]
        related_df.at[1, "ref_id"] = df.iloc[1][source.schema.event_id]

        related = Eventstream(
            raw_data_schema=test_schema_1,
            raw_data=related_df,
            schema=EventstreamSchema(),
            relations=[{"eventstream": source, "raw_col": "ref_id"}],
        )
        click_1_df = related.to_dataframe()

        click_1_df = click_1_df[click_1_df[related.schema.event_name] == "click_1"]
        related._soft_delete(click_1_df)

        source._join_eventstream(related)
        result_df = source.to_dataframe()
        result_events_names: list[str] = result_df[source.schema.event_name].to_list()

        assert result_events_names == ["pageview", "click_2"]

        with_deleted_events = source.to_dataframe(show_deleted=True)

        deleted_events = with_deleted_events[with_deleted_events[DELETE_COL_NAME] == True]
        deleted_events_names: list[str] = deleted_events[source.schema.event_name].to_list()

        assert deleted_events_names == []

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
        )

        actual = stream.to_dataframe()
        expected = test_df_identical_timestamps_corr.copy()
        expected.timestamp = pd.to_datetime(expected.timestamp)

        assert pd.testing.assert_frame_equal(actual[expected.columns], expected) is None
