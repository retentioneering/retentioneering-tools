from __future__ import annotations

from typing import List

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
from retentioneering.data_processors_lib.split_sessions import (
    SplitSessions,
    SplitSessionsParams,
)
from retentioneering.eventstream.eventstream import Eventstream, EventstreamSchema
from retentioneering.eventstream.schema import RawDataSchema
from retentioneering.params_model import ParamsModel
from retentioneering.preprocessing_graph import PreprocessingGraph
from retentioneering.preprocessing_graph.nodes import (
    EventsNode,
    MergeNode,
    Node,
    SourceNode,
)
from tests.preprocessing_graph.fixtures.stub_processorpgraph import stub_processorpgraph


class TestPreprocessingGraph:
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

    def mock_events_node(self, StubProcessorPreprocessingGraph: DataProcessor, StubPProcessorParams: ParamsModel):
        return EventsNode(processor=StubProcessorPreprocessingGraph(params=StubPProcessorParams(a="a")))  # type: ignore

    def create_graph(self):
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
        )

        return PreprocessingGraph(source_stream=source)

    def test_export_stub(self, stub_processorpgraph) -> None:
        StubPProcessorParams: ParamsModel = stub_processorpgraph["params"]
        StubProcessorPreprocessingGraph: DataProcessor = stub_processorpgraph["processor"]

        params = StubPProcessorParams(a="a")  # type: ignore
        processor = StubProcessorPreprocessingGraph(params=params)

        assert {
            "name": "StubProcessorPreprocessingGraph",
            "params": [{"name": "A", "params": ["a", "b"], "default": None, "widget": "enum", "optional": False}],
        } == processor.get_view()

    def test_create_graph(self):
        self.setUp()
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
        )

        graph = PreprocessingGraph(source_stream=source)

        assert isinstance(graph.root, SourceNode)
        assert graph.root.events == source

    def test_get_parents(self, stub_processorpgraph):
        self.setUp()
        StubPProcessorParams: ParamsModel = stub_processorpgraph["params"]
        StubProcessorPreprocessingGraph: DataProcessor = stub_processorpgraph["processor"]

        graph = self.create_graph()

        added_nodes: List[Node] = []

        for _ in range(5):
            new_node = self.mock_events_node(StubProcessorPreprocessingGraph, StubPProcessorParams)
            added_nodes.append(new_node)

            graph.add_node(node=new_node, parents=[graph.root])
            new_node_parents = graph.get_parents(new_node)

            assert len(new_node_parents) == 1
            assert new_node_parents[0] == graph.root

        merge_node = MergeNode()

        graph.add_node(
            node=merge_node,
            parents=added_nodes,
        )

        merge_node_parents = graph._get_merge_node_parents(merge_node)
        assert len(merge_node_parents) == len(added_nodes)

        for node in merge_node_parents:
            assert node in added_nodes

        merge_node_parents = graph.get_parents(merge_node)

        assert len(merge_node_parents) == len(added_nodes)
        for merge_node_parent in merge_node_parents:
            assert merge_node_parent in added_nodes

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
            add_start_end_events=False,
        )

        def cart_func(df: pd.DataFrame, schema: EventstreamSchema) -> pd.Series[bool]:
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        cart_events = EventsNode(GroupEvents(GroupEventsParams(event_name="add_to_cart", func=cart_func)))  # type: ignore

        graph = PreprocessingGraph(source)
        graph.add_node(node=cart_events, parents=[graph.root])
        result = graph.combine(cart_events)
        result_df = result.to_dataframe()

        event_names: list[str] = result_df[source.schema.event_name].to_list()
        event_types: list[str] = result_df[source.schema.event_type].to_list()

        assert event_names == ["pageview", "add_to_cart", "pageview", "add_to_cart"]
        assert event_types == ["raw", "group_alias", "raw", "group_alias"]

    def test_combine_merge_node_with_groups(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:06", "user_id": "2"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:07", "user_id": "2"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            add_start_end_events=False,
        )

        allowed_events = EventsNode(
            FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] != "trash_event"))
        )

        user_1 = EventsNode(FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.user_id] == "1")))

        cart_events_1 = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="add_to_cart",
                    func=lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
                )
            )
        )

        logout_events_1 = EventsNode(
            processor=GroupEvents(
                GroupEventsParams(
                    event_name="logout",
                    func=lambda df, schema: df[schema.event_name] == "exit_btn_click",
                )
            ),
            description="logout_events_1",
        )

        user_2 = EventsNode(FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.user_id] == "2")))

        cart_events_2 = cart_events_1.copy()
        logout_events_2 = logout_events_1.copy()

        merge = MergeNode()

        graph = PreprocessingGraph(source)
        graph.add_node(node=allowed_events, parents=[graph.root])
        graph.add_node(node=user_1, parents=[allowed_events])
        graph.add_node(node=cart_events_1, parents=[user_1])
        graph.add_node(node=logout_events_1, parents=[cart_events_1])

        graph.add_node(node=user_2, parents=[allowed_events])
        graph.add_node(node=cart_events_2, parents=[user_2])
        graph.add_node(node=logout_events_2, parents=[cart_events_2])

        graph.add_node(
            node=merge,
            parents=[
                logout_events_1,
                logout_events_2,
            ],
        )

        result = graph.combine(merge)
        result_df = result.to_dataframe()

        event_names: list[str] = result_df[source.schema.event_name].to_list()
        event_types: list[str] = result_df[source.schema.event_type].to_list()
        user_ids: list[str] = result_df[source.schema.user_id].to_list()

        assert event_names == [
            "pageview",
            "add_to_cart",
            "pageview",
            "logout",
            "add_to_cart",
        ]

        assert event_types == ["raw", "group_alias", "raw", "group_alias", "group_alias"]

        assert user_ids == ["1", "1", "1", "2", "2"]

    def test_combine_merge_node_delete_events(self) -> None:
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "1"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            add_start_end_events=False,
        )

        do_nothing_node_1 = EventsNode(
            processor=FilterEvents(
                FilterEventsParams(func=lambda df, schema: df[schema.user_id] == "1"),
            ),
            description="do_nothing_node_1",
        )

        delete_trash_events_1 = EventsNode(
            processor=FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] != "trash_event")),
            description="delete_trash_events_1",
        )

        do_nothing_node_2 = EventsNode(
            FilterEvents(
                FilterEventsParams(func=lambda df, schema: df[schema.user_id] == "1"),
            )
        )

        delete_trash_events_2 = EventsNode(
            FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] != "trash_event"))
        )

        merge = MergeNode()
        graph = PreprocessingGraph(source)
        graph.add_node(node=do_nothing_node_1, parents=[graph.root])
        graph.add_node(node=do_nothing_node_2, parents=[graph.root])
        graph.add_node(node=delete_trash_events_1, parents=[do_nothing_node_1])
        graph.add_node(node=delete_trash_events_2, parents=[do_nothing_node_2])

        graph.add_node(
            node=merge,
            parents=[
                delete_trash_events_1,
                delete_trash_events_2,
            ],
        )

        result = graph.combine(merge)
        result_df = result.to_dataframe()

        event_names: list[str] = result_df[source.schema.event_name].to_list()

        assert event_names == ["pageview", "cart_btn_click", "pageview", "exit_btn_click", "plus_icon_click"]

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
        graph = PreprocessingGraph(source)
        root_node = [x for x in graph._ngraph][0]
        root_node.pk = "0dc3b706-e6cc-401e-96f7-6a45d3947d5c"
        cart_events = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="add_to_cart",
                    func=lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
                )
            ),
            description="some description",
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
                    "description": "some description",
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

        export_data = graph._export_handler(payload={})
        # del export_data["links"]
        # При каждом запуске функции имеют разные адреса, отсюда разница при ассерте
        del export_data["nodes"][1]["processor"]["values"]["func"]
        del export_data["nodes"][2]["processor"]["values"]["func"]

        assert example == export_data

    def test_index_order_inheritance(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "1"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        custom_index_order = [
            "session_start",
            "group_alias",
            "profile",
            "path_start",
            "new_user",
            "existing_user",
            "cropped_left",
            "session_start_cropped",
            "raw",
            "raw_sleep",
            None,
            "synthetic",
            "synthetic_sleep",
            "positive_target",
            "negative_target",
            "session_end_cropped",
            "session_end",
            "session_sleep",
            "cropped_right",
            "absent_user",
            "lost_user",
            "path_end",
        ]

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            index_order=custom_index_order,
        )

        group_1 = EventsNode(
            FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] == "pageview"))
        )

        group_2 = EventsNode(
            FilterEvents(FilterEventsParams(func=lambda df, schema: df[schema.event_name] != "pageview"))
        )

        group_cart = EventsNode(
            GroupEvents(
                GroupEventsParams(
                    event_name="add_to_cart",
                    func=lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
                )
            )
        )

        merge = MergeNode()

        split_sessions = EventsNode(
            SplitSessions(
                SplitSessionsParams(
                    timeout=(1, "h"),
                    mark_truncated=True,
                ),
            )
        )

        graph = PreprocessingGraph(source_stream=source)

        graph.add_node(node=group_1, parents=[graph.root])

        graph.add_node(node=group_2, parents=[graph.root])

        graph.add_node(node=group_cart, parents=[group_2])

        graph.add_node(node=merge, parents=[group_cart, group_1])

        graph.add_node(node=split_sessions, parents=[merge])

        result_stream = graph.combine(split_sessions)

        assert result_stream.index_order == custom_index_order

    def test_display(self):
        self.setUp()
        source = Eventstream(
            raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema()
        )

        graph = PreprocessingGraph(source_stream=source)
        display = graph.display()
        assert None is display
