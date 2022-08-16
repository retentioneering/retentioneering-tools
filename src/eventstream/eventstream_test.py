from __future__ import annotations

import unittest
import uuid

import pandas as pd

from src.utils.pandas import shuffle_df

from .eventstream import DELETE_COL_NAME, Eventstream
from .schema import EventstreamSchema, RawDataSchema

# from src.eventstream.eventstream import DELETE_COL_NAME
# from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


class EventstreamTest(unittest.TestCase):
    __raw_data: pd.DataFrame
    __raw_data_schema: RawDataSchema

    def setUp(self):
        self.__raw_data = pd.DataFrame(
            [
                {
                    "name": "pageview",
                    "event_timestamp": "2021-10-26 12:00",
                    "user_id": "1",
                },
                {
                    "name": "click_1",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                },
                {
                    "name": "click_2",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                },
            ]
        )
        self.__raw_data_schema = RawDataSchema(event_name="name", event_timestamp="event_timestamp", user_id="user_id")

    def test_create_eventstream(self):
        es = Eventstream(
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
            schema=EventstreamSchema(),
        )

        df = es.to_dataframe()
        schema = es.schema
        columns = df.columns

        self.assertTrue(schema.event_id in columns)
        self.assertTrue(schema.event_type in columns)
        self.assertTrue(schema.event_name in columns)
        self.assertTrue(schema.event_timestamp in columns)
        self.assertTrue(schema.user_id in columns)

        for [_, event] in df.iterrows():  # type: ignore
            self.assertEqual(event[schema.event_type], "raw")
            assert isinstance(event[schema.event_id], uuid.UUID)

    def test_create_custom_cols(self):
        custom_cols = ["custom_col_1", "custom_col_2"]

        es = Eventstream(
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
            schema=EventstreamSchema(custom_cols=custom_cols),
        )

        df = es.to_dataframe()

        self.assertEqual(es.schema.custom_cols, custom_cols)

        for custom_col in custom_cols:
            self.assertTrue(custom_col in df.columns)

    def test_get_prepared_custom_col(self):
        custom_cols = ["custom_col_1", "custom_col_2"]

        raw_data = self.__raw_data.copy()
        raw_data[custom_cols[0]] = "custom_col_value"
        raw_data[custom_cols[1]] = "custom_col_value"

        raw_data_schema = self.__raw_data_schema.copy()
        raw_data_schema.custom_cols = [{"custom_col": col, "raw_data_col": col} for col in custom_cols]

        es = Eventstream(
            raw_data=raw_data,
            raw_data_schema=raw_data_schema,
            schema=EventstreamSchema(custom_cols=custom_cols),
        )

        schema = es.schema

        df = es.to_dataframe()
        for custom_col in custom_cols:
            self.assertTrue(custom_col in df.columns)

        for [_, event] in df.iterrows():  # type: ignore
            self.assertEqual(event[schema.custom_cols[0]], "custom_col_value")
            self.assertEqual(event[schema.custom_cols[1]], "custom_col_value")

    def test_index_events(self):
        raw_data = pd.DataFrame(
            [
                {
                    "name": "click_1",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                    "type": "raw",
                },
                {
                    "name": "pageview",
                    "event_timestamp": "2021-10-26 12:00",
                    "user_id": "1",
                    "type": "raw",
                },
                {
                    "name": "pause",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                    "type": "pause",
                },
                {
                    "name": "click_2",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                    "type": "raw",
                },
                {
                    "name": "end",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                    "type": "end",
                },
                {
                    "name": "start",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                    "type": "start",
                },
            ]
        )
        schema = self.__raw_data_schema.copy()
        schema.event_type = "type"

        es = Eventstream(raw_data=raw_data, raw_data_schema=schema, schema=EventstreamSchema())
        df: pd.DataFrame = es.to_dataframe()

        names: list[str] = [event[es.schema.event_name] for [_, event] in df.iterrows()]  # type: ignore
        self.assertListEqual(names, ["pageview", "click_1", "start", "click_2", "pause", "end"])

    def test_create_relation(self):
        es = Eventstream(
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
            schema=EventstreamSchema(),
        )

        # shuffle data
        df = es.to_dataframe()
        parent_df = shuffle_df(df)
        parent_df["ref"] = parent_df[es.schema.event_id]

        related_es = Eventstream(
            raw_data=parent_df,
            raw_data_schema=RawDataSchema(
                event_name=es.schema.event_name,
                event_type=es.schema.event_type,
                event_timestamp=es.schema.event_timestamp,
                user_id=es.schema.user_id,
            ),
            schema=EventstreamSchema(event_name="name"),
            relations=[{"evenstream": es, "raw_col": "ref"}],
        )

        related_df = related_es.to_dataframe()

        for [_, event] in related_df.iterrows():  # type: ignore
            query: pd.Series[str] = parent_df[es.schema.event_id] == event["ref_0"]
            related_event = parent_df[query].iloc[0]  # type: ignore
            self.assertEqual(related_event[es.schema.event_id], event["ref_0"])

            self.assertEqual(
                related_event[es.schema.event_name],
                event[related_es.schema.event_name],
            )

            self.assertEqual(
                related_event[es.schema.event_timestamp],
                event[related_es.schema.event_timestamp],
            )

            self.assertEqual(related_event[es.schema.user_id], event[related_es.schema.user_id])

    def test_join_eventstream(self):
        schema = EventstreamSchema(
            event_id="id",
            event_name="event_name",
            event_timestamp="event_timestamp",
            user_id="user_id",
            event_index="index",
            event_type="type",
            custom_cols=[],
        )

        source_schema = RawDataSchema(
            event_name="event_name",
            event_timestamp="event_timestamp",
            user_id="user_id",
        )

        child_schema = RawDataSchema(
            event_name="name",
            event_timestamp="event_timestamp",
            user_id="user_id",
            event_type="type",
        )

        source_df = pd.DataFrame(
            [
                {
                    "event_name": "pageview",
                    "event_timestamp": "2021-10-26 12:00",
                    "user_id": "1",
                },
                {
                    "event_name": "cart_btn_click",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                },
                {
                    "event_name": "pageview",
                    "event_timestamp": "2021-10-26 12:03",
                    "user_id": "1",
                },
                {
                    "event_name": "plus_icon_click",
                    "event_timestamp": "2021-10-26 12:04",
                    "user_id": "1",
                },
            ]
        )

        source = Eventstream(
            raw_data=source_df,
            schema=schema,
            raw_data_schema=source_schema,
        )

        source_events_df = source.to_dataframe()

        joined_df = pd.DataFrame(
            data=[  # type: ignore
                {
                    "type": "synthetic",
                    "name": "add_to_cart",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                    "ref_id": source_events_df.iloc[1]["id"],
                },
                {
                    "type": "synthetic",
                    "name": "add_to_cart",
                    "event_timestamp": "2021-10-26 12:04",
                    "user_id": "1",
                    "ref_id": source_events_df.iloc[3]["id"],
                },
                {
                    "type": "synthetic",
                    "name": "add_to_cart",
                    "event_timestamp": "2021-10-26 12:09",
                    "user_id": "1",
                    "ref_id": uuid.uuid4(),
                },
                {
                    "type": "synthetic",
                    "name": "add_to_cart123",
                    "event_timestamp": "2021-10-26 12:10",
                    "user_id": "2",
                    "ref_id": None,
                },
            ]
        )

        child = Eventstream(
            raw_data=joined_df,
            schema=schema,
            raw_data_schema=child_schema,
            relations=[{"evenstream": source, "raw_col": "ref_id"}],
        )

        child_events_df = child.to_dataframe()

        source.join_eventstream(child)
        result_df = source.to_dataframe()

        names: list[str] = result_df[schema.event_name].to_list()
        timestamps: list[int] = result_df[schema.event_timestamp].to_list()
        user_ids: list[str] = result_df[schema.user_id].to_list()
        types: list[str] = result_df[schema.event_type].to_list()

        self.assertEqual(
            names,
            ["pageview", "add_to_cart", "pageview", "add_to_cart", "add_to_cart123"],
        )

        self.assertEqual(types, ["raw", "synthetic", "raw", "synthetic", "synthetic"])

        expected_timestamps: list[int] = [
            source_events_df.iloc[0][schema.event_timestamp],
            child_events_df.iloc[0][schema.event_timestamp],
            source_events_df.iloc[2][schema.event_timestamp],
            child_events_df.iloc[1][schema.event_timestamp],
            child_events_df.iloc[3][schema.event_timestamp],
        ]

        self.assertEqual(timestamps, expected_timestamps)
        self.assertEqual(user_ids, ["1", "1", "1", "1", "2"])

        result_with_deleted = source.to_dataframe(show_deleted=True)
        deleted_events = result_with_deleted[result_with_deleted[DELETE_COL_NAME] == True]
        deleted_events_names: list[str] = deleted_events[schema.event_name].to_list()

        self.assertEqual(deleted_events_names, ["cart_btn_click", "plus_icon_click"])

    def test_soft_delete(self):
        source = Eventstream(
            schema=EventstreamSchema(),
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
        )
        df = source.to_dataframe()

        source.soft_delete(events=df[df[source.schema.event_name] == "pageview"])

        after_delete = source.to_dataframe()
        after_delete_all = source.to_dataframe(show_deleted=True)

        event_names: list[str] = after_delete[source.schema.event_name].to_list()
        with_deleted_event_names: list[str] = after_delete_all[source.schema.event_name].to_list()

        self.assertEqual(event_names, ["click_1", "click_2"])

        self.assertEqual(with_deleted_event_names, ["pageview", "click_1", "click_2"])

        source.soft_delete(events=after_delete[after_delete[source.schema.event_name] == "click_1"])

        after_delete = source.to_dataframe()
        after_delete_all = source.to_dataframe(show_deleted=True)

        event_names: list[str] = after_delete[source.schema.event_name].to_list()
        with_deleted_event_names: list[str] = after_delete_all[source.schema.event_name].to_list()

        self.assertEqual(event_names, ["click_2"])

        self.assertEqual(with_deleted_event_names, ["pageview", "click_1", "click_2"])

    def test_delete_events_by_join(self):
        source = Eventstream(
            schema=EventstreamSchema(),
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
        )
        df = source.to_dataframe()

        source.soft_delete(events=df[df[source.schema.event_name] == "pageview"])

        related_cols: list[str] = self.__raw_data.columns.to_list()  # type: ignore
        related_cols.append("ref_id")
        related_df = pd.DataFrame(self.__raw_data.copy(), columns=related_cols)
        related_df = related_df[(related_df["name"] == "pageview") | (related_df["name"] == "click_1")]
        related_df.reset_index(inplace=True, drop=True)
        related_df.at[0, "ref_id"] = df.iloc[0][source.schema.event_id]  # type: ignore
        related_df.at[1, "ref_id"] = df.iloc[1][source.schema.event_id]  # type: ignore

        related = Eventstream(
            schema=EventstreamSchema(),
            raw_data=related_df,
            raw_data_schema=self.__raw_data_schema,
            relations=[{"evenstream": source, "raw_col": "ref_id"}],
        )
        click_1_df = related.to_dataframe()

        click_1_df = click_1_df[click_1_df[related.schema.event_name] == "click_1"]
        related.soft_delete(click_1_df)

        source.join_eventstream(related)
        result_df = source.to_dataframe()
        result_events_names: list[str] = result_df[source.schema.event_name].to_list()

        self.assertEqual(result_events_names, ["click_2"])

        with_deleted_events = source.to_dataframe(show_deleted=True)

        deleted_events = with_deleted_events[with_deleted_events[DELETE_COL_NAME] == True]
        deleted_events_names: list[str] = deleted_events[source.schema.event_name].to_list()

        self.assertEqual(deleted_events_names, ["pageview", "pageview", "click_1", "click_1"])
