from __future__ import annotations

import unittest
from typing import List, Literal, Union

import pandas as pd

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import EventstreamSchema, Eventstream
from src.eventstream.schema import RawDataSchema
from src.params_model import ParamsModel
from .p_graph import SourceNode, MergeNode, PGraph, EventsNode, Node
from ..data_processors_lib.simple_processors.delete_events import DeleteEvents, DeleteEventsParams
from ..data_processors_lib.simple_processors.simple_group import SimpleGroup, SimpleGroupParams


class StubProcessorParams(ParamsModel):
    a: Union[Literal["a"], Literal["b"]]


class StubProcessor(DataProcessor):
    params: StubProcessorParams

    def __init__(self, params: StubProcessorParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        return eventstream.copy()


class EventstreamTest(unittest.TestCase):
    __raw_data: pd.DataFrame
    __raw_data_schema: RawDataSchema

    def setUp(self):
        self.__raw_data = pd.DataFrame([
            {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
            {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
            {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
        ])
        self.__raw_data_schema = RawDataSchema(
            event_name="name", event_timestamp="event_timestamp", user_id="user_id")

    def mock_events_node(self):
        return EventsNode(processor=StubProcessor(params=StubProcessorParams(**{
            "a": "a"
        })))

    def create_graph(self):
        source = Eventstream(
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
            schema=EventstreamSchema()
        )

        return PGraph(source_stream=source)

    def test_create_graph(self):
        source = Eventstream(
            raw_data=self.__raw_data,
            raw_data_schema=self.__raw_data_schema,
            schema=EventstreamSchema()
        )

        graph = PGraph(source_stream=source)

        self.assertIsInstance(graph.root, SourceNode)
        self.assertEqual(graph.root.events, source)

    def test_get_parents(self):
        graph = self.create_graph()

        added_nodes: List[Node] = []

        for _ in range(5):
            new_node = self.mock_events_node()
            added_nodes.append(new_node)

            graph.add_node(
                node=new_node,
                parents=[graph.root]
            )
            new_node_parents = graph.get_parents(new_node)

            self.assertEqual(len(new_node_parents), 1)
            self.assertEqual(new_node_parents[0], graph.root)

        merge_node = MergeNode()

        graph.add_node(
            node=merge_node,
            parents=added_nodes,
        )

        merge_node_parents = graph.get_merge_node_parents(merge_node)
        self.assertEqual(len(merge_node_parents), len(added_nodes))

        for node in merge_node_parents:
            self.assertTrue(node in added_nodes)

        merge_node_parents = graph.get_parents(merge_node)

        self.assertEqual(len(merge_node_parents), len(added_nodes))
        for merge_node_parent in merge_node_parents:
            self.assertTrue(merge_node_parent in added_nodes)

    def test_combine_events_node(self):
        source_df = pd.DataFrame([
            {"event_name": "pageview",
             "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
            {"event_name": "cart_btn_click",
             "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
            {"event_name": "pageview",
             "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            {"event_name": "plus_icon_click",
             "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
        ])

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event_name",
                event_timestamp="event_timestamp",
                user_id="user_id"
            )
        )

        cart_events = EventsNode(
            SimpleGroup(params=SimpleGroupParams(**{
                "event_name": "add_to_cart",
                "filter": lambda df, schema: df[schema.event_name].isin(
                    ["cart_btn_click", "plus_icon_click"]),
            }))
        )

        graph = PGraph(source)
        graph.add_node(
            node=cart_events,
            parents=[graph.root]
        )
        result = graph.combine(cart_events)
        result_df = result.to_dataframe()

        event_names = result_df[source.schema.event_name].to_list()
        event_types = result_df[source.schema.event_type].to_list()

        self.assertEqual(event_names, [
            "pageview", "add_to_cart", "pageview", "add_to_cart"
        ])
        self.assertEqual(event_types, [
            "raw", "group_alias", "raw", "group_alias"
        ])

    def test_combine_merge_node(self):
        source_df = pd.DataFrame([
            {"event_name": "pageview",
             "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
            {"event_name": "cart_btn_click",
             "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
            {"event_name": "pageview",
             "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            {"event_name": "trash_event",
             "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            {"event_name": "exit_btn_click",
             "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
            {"event_name": "plus_icon_click",
             "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
        ])

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event_name",
                event_timestamp="event_timestamp",
                user_id="user_id"
            )
        )

        cart_events = EventsNode(
            SimpleGroup(SimpleGroupParams(**{
                "event_name": "add_to_cart",
                "filter": lambda df, schema: df[schema.event_name].isin(
                    ["cart_btn_click", "plus_icon_click"])
            }))
        )
        logout_events = EventsNode(
            SimpleGroup(SimpleGroupParams(**{
                "event_name": "logout",
                "filter": lambda df, schema: df[schema.event_name] == "exit_btn_click"
            }))
        )
        trash_events = EventsNode(
            DeleteEvents(DeleteEventsParams(**{
                "filter": lambda df, schema: df[schema.event_name] == "trash_event"
            }))
        )
        merge = MergeNode()

        graph = PGraph(source)
        graph.add_node(
            node=cart_events,
            parents=[graph.root]
        )
        graph.add_node(
            node=logout_events,
            parents=[graph.root]
        )
        graph.add_node(
            node=trash_events,
            parents=[graph.root]
        )
        graph.add_node(
            node=merge,
            parents=[
                cart_events,
                logout_events,
                trash_events,
            ]
        )

        result = graph.combine(merge)
        result_df = result.to_dataframe()

        event_names = result_df[source.schema.event_name].to_list()
        event_types = result_df[source.schema.event_type].to_list()
        user_ids = result_df[source.schema.user_id].to_list()

        self.assertEqual(
            event_names,
            [
                "pageview",
                "add_to_cart",
                "pageview",
                "logout",
                "add_to_cart",
            ]
        )

        self.assertEqual(
            event_types,
            [
                "raw",
                "group_alias",
                "raw",
                "group_alias",
                "group_alias"
            ],
        )

        self.assertEqual(
            user_ids,
            ["1", "1", "1", "2", "1"]
        )
