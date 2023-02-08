from __future__ import annotations

import unittest
from typing import List, Literal, Union

import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processors_lib.filter_events import (
    FilterEvents,
    FilterEventsParams,
)
from retentioneering.data_processors_lib.group_events import (
    GroupEvents,
    GroupEventsParams,
)
from retentioneering.eventstream.eventstream import Eventstream, EventstreamSchema
from retentioneering.eventstream.schema import RawDataSchema
from retentioneering.graph.nodes import EventsNode, MergeNode, Node, SourceNode
from retentioneering.graph.p_graph import PGraph
from retentioneering.params_model import ParamsModel


class StubPProcessorParams(ParamsModel):
    a: Union[Literal["a"], Literal["b"]]


class StubProcessorPGraph(DataProcessor):
    params: StubPProcessorParams

    def __init__(self, params: StubPProcessorParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        return eventstream.copy()


class EventstreamTest(unittest.TestCase):
    __raw_data: pd.DataFrame
    __raw_data_schema: RawDataSchema

    def setUp(self):
        self.__raw_data = pd.DataFrame(
            [
                {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
            ]
        )
        self.__raw_data_schema = RawDataSchema(event_name="name", event_timestamp="event_timestamp", user_id="user_id")

    def mock_events_node(self):
        return EventsNode(processor=StubProcessorPGraph(params=StubPProcessorParams(a="a")))

    def create_graph(self):
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
        )

        return PGraph(source_stream=source)

    def test_export_stub(self) -> None:

        params = StubPProcessorParams(a="a")
        processor = StubProcessorPGraph(params=params)

        assert {
            "name": "StubProcessorPGraph",
            "params": [{"name": "A", "params": ["a", "b"], "default": None, "widget": "enum", "optional": False}],
        } == processor.get_view()

    def test_create_graph(self):
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
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

            graph.add_node(node=new_node, parents=[graph.root])
            new_node_parents = graph.get_parents(new_node)

            self.assertEqual(len(new_node_parents), 1)
            self.assertEqual(new_node_parents[0], graph.root)

        merge_node = MergeNode()

        graph.add_node(
            node=merge_node,
            parents=added_nodes,
        )

        merge_node_parents = graph._get_merge_node_parents(merge_node)
        self.assertEqual(len(merge_node_parents), len(added_nodes))

        for node in merge_node_parents:
            self.assertTrue(node in added_nodes)

        merge_node_parents = graph.get_parents(merge_node)

        self.assertEqual(len(merge_node_parents), len(added_nodes))
        for merge_node_parent in merge_node_parents:
            self.assertTrue(merge_node_parent in added_nodes)

    def test_combine_events_node(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )

        def cart_func(df: pd.DataFrame, schema: EventstreamSchema) -> pd.Series[bool]:
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        cart_events = EventsNode(GroupEvents(GroupEventsParams(event_name="add_to_cart", func=cart_func)))

        graph = PGraph(source)
        graph.add_node(node=cart_events, parents=[graph.root])
        result = graph.combine(cart_events)
        result_df = result.to_dataframe()

        event_names: list[str] = result_df[source.schema.event_name].to_list()
        event_types: list[str] = result_df[source.schema.event_type].to_list()

        self.assertEqual(event_names, ["pageview", "add_to_cart", "pageview", "add_to_cart"])
        self.assertEqual(event_types, ["raw", "group_alias", "raw", "group_alias"])

    def test_combine_merge_node(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )

        cart_events = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="add_to_cart",
                    func=lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
                )
            )
        )
        logout_events = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="logout",
                    func=lambda df, schema: df[schema.event_name] == "exit_btn_click",
                )
            )
        )
        allowed_events = EventsNode(
            FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] != "trash_event"))
        )
        merge = MergeNode()

        graph = PGraph(source)
        graph.add_node(node=cart_events, parents=[graph.root])
        graph.add_node(node=logout_events, parents=[graph.root])
        graph.add_node(node=allowed_events, parents=[graph.root])
        graph.add_node(
            node=merge,
            parents=[
                cart_events,
                logout_events,
                allowed_events,
            ],
        )

        result = graph.combine(merge)
        result_df = result.to_dataframe()

        event_names: list[str] = result_df[source.schema.event_name].to_list()
        event_types: list[str] = result_df[source.schema.event_type].to_list()
        user_ids: list[str] = result_df[source.schema.user_id].to_list()

        self.assertEqual(
            event_names,
            [
                "pageview",
                "add_to_cart",
                "pageview",
                "logout",
                "add_to_cart",
            ],
        )

        self.assertEqual(
            event_types,
            ["raw", "group_alias", "raw", "group_alias", "group_alias"],
        )

        self.assertEqual(user_ids, ["1", "1", "1", "2", "1"])

    def test_get_values(self) -> None:
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )
        graph = PGraph(source)
        root_node = [x for x in graph._ngraph][0]
        root_node.pk = "0dc3b706-e6cc-401e-96f7-6a45d3947d5c"
        cart_events = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="add_to_cart",
                    func=lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
                )
            )
        )
        cart_events.pk = "07921cb0-60b8-45af-928d-272d1b622b25"
        logout_events = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="logout",
                    func=lambda df, schema: df[schema.event_name] == "exit_btn_click",
                )
            )
        )
        logout_events.pk = "114251ae-0f03-45e6-a163-af51bb02dfd5"

        graph.add_node(node=cart_events, parents=[graph.root])
        graph.add_node(node=logout_events, parents=[cart_events])

        example = {
            "directed": True,
            "nodes": [
                {"name": "SourceNode", "pk": "0dc3b706-e6cc-401e-96f7-6a45d3947d5c"},
                {
                    "name": "EventsNode",
                    "pk": "07921cb0-60b8-45af-928d-272d1b622b25",
                    "processor": {
                        "name": "GroupEvents",
                        "values": {"event_name": "add_to_cart", "event_type": "group_alias"},
                    },
                },
                {
                    "name": "EventsNode",
                    "pk": "114251ae-0f03-45e6-a163-af51bb02dfd5",
                    "processor": {
                        "name": "GroupEvents",
                        "values": {"event_name": "logout", "event_type": "group_alias"},
                    },
                },
            ],
            "links": [
                {"source": "0dc3b706-e6cc-401e-96f7-6a45d3947d5c", "target": "07921cb0-60b8-45af-928d-272d1b622b25"},
                {"source": "07921cb0-60b8-45af-928d-272d1b622b25", "target": "114251ae-0f03-45e6-a163-af51bb02dfd5"},
            ],
        }

        export_data = graph.export(payload={})
        # del export_data["links"]
        # При каждом запуске функции имеют разные адреса, отсюда разница при ассерте
        del export_data["nodes"][1]["processor"]["values"]["func"]
        del export_data["nodes"][2]["processor"]["values"]["func"]

        assert example == export_data

    def test_display(self):
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
        )

        graph = PGraph(source_stream=source)
        display = graph.display()
        assert None is display
