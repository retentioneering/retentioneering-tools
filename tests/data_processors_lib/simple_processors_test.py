from __future__ import annotations

import unittest

import pandas as pd

from src.data_processors_lib.simple_processors.filter_events import (
    FilterEvents,
    FilterEventsParams,
)
from src.data_processors_lib.simple_processors.simple_group import (
    SimpleGroup,
    SimpleGroupParams,
)
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import PGraph, EventsNode


class SimpleProcessorsTest(unittest.TestCase):
    def test_simple_group(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:04", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        source_df = source.to_dataframe()

        def filter_(df: pd.DataFrame, schema: EventstreamSchema) -> pd.Series[bool]:
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        params = SimpleGroupParams(
            event_name="add_to_cart",
            event_type="group_alias",
            filter=filter_,
        )

        group = SimpleGroup(params=params)

        result = group.apply(source)
        result_df = result.to_dataframe()

        events_names: list[str] = result_df[result.schema.event_name].to_list()
        events_type: list[str] = result_df[result.schema.event_type].to_list()
        refs: list[str] = result_df["ref_0"].to_list()

        self.assertEqual(events_names, ["add_to_cart", "add_to_cart"])
        self.assertEqual(events_type, ["group_alias", "group_alias"])
        self.assertEqual(refs, [source_df.iloc[1][source.schema.event_id], source_df.iloc[3][source.schema.event_id]])

    def test_delete_factory(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:04", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        def filter_(df: pd.DataFrame, schema: EventstreamSchema) -> pd.Series[bool]:
            return ~df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        delete_factory = FilterEvents(FilterEventsParams(filter=filter_))

        result = delete_factory.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names: list[str] = result_df[result.schema.event_name].to_list()

        self.assertEqual(events_names, ["cart_btn_click", "plus_icon_click"])


class TestSimpleProcessorsGraph():
    def test_simple_group_graph(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:00:00'],
            [1, 'event2', '2022-01-01 00:00:01'],
            [1, 'event3', '2022-01-01 00:00:02'],
            [2, 'event4', '2022-01-02 00:00:00'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']
        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:00:00'],
            [1, 'event_new', 'group_alias', '2022-01-01 00:00:01'],
            [1, 'event3', 'raw', '2022-01-01 00:00:02'],
            [2, 'event_new', 'group_alias', '2022-01-02 00:00:00'],
        ], columns=correct_result_columns
        )
        stream = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name='event', event_timestamp='timestamp', user_id='user_id'
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        def filter_(df, schema):
            return (df[schema.user_id].isin([2])) | (df.event_name.str.contains('event2'))

        graph = PGraph(source_stream=stream)

        event_agg = EventsNode(
            SimpleGroup(params=SimpleGroupParams(**{
                'event_name': 'event_new',
                'filter': filter_
            }))
        )

        graph.add_node(node=event_agg, parents=[graph.root])
        res = graph.combine(node=event_agg).to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert res.compare(correct_result).shape == (0, 0)

    def test_delete_graph(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:00:00'],
            [1, 'event2', '2022-01-01 00:00:01'],
            [1, 'event3', '2022-01-01 00:00:02'],
            [2, 'event4', '2022-01-02 00:00:00'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        correct_result_columns = ['user_id', 'event_name', 'event_type', 'event_timestamp']
        correct_result = pd.DataFrame([
            [1, 'event1', 'raw', '2022-01-01 00:00:00'],
            [1, 'event3', 'raw', '2022-01-01 00:00:02'],
        ], columns=correct_result_columns
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name='event', event_timestamp='timestamp', user_id='user_id'
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )
        stream_orig = stream.to_dataframe()

        def filter_(df, schema):
            return ~((df[schema.user_id].isin([2])) |
                    (df.event_name.str.contains('event2')))

        graph = PGraph(source_stream=stream)

        delete_conditional = EventsNode(FilterEvents(
            params=FilterEventsParams(filter=filter_)))

        graph.add_node(node=delete_conditional, parents=[graph.root])
        res = graph.combine(node=delete_conditional).to_dataframe()[correct_result_columns].reset_index(
            drop=True)

        assert res.compare(correct_result).shape == (0, 0)
        # checking that the original stream remains immutable
        assert stream_orig.compare(stream.to_dataframe()).shape == (0, 0)
