import pandas as pd

from src.data_processors_lib.rete import StartEndEvents, StartEndEventsParams
from src.eventstream.eventstream import Eventstream, RawDataSchema
from src.graph.nodes import EventsNode
from src.graph.p_graph import PGraph


class TestPGraphExportImport:
    def create_graph(self) -> PGraph:
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

        return graph

    def test_start_end_export(self) -> None:
        graph = self.create_graph()
        node = EventsNode(processor=StartEndEvents(params=StartEndEventsParams(**{})))
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export(payload={})
        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]

        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {"name": "EventsNode", "processor": {"values": {}, "name": "StartEndEvents"}},
            ],
        } == export_data

    def test_start_end_import(self) -> None:
        graph = self.create_graph()

        graph._set_graph(
            payload={
                "directed": True,
                "nodes": [
                    {"name": "SourceNode", "pk": "a911d6de-48a9-4898-8d4f-c123efc84498"},
                    {
                        "name": "EventsNode",
                        "pk": "81e5ead2-c0ed-43c5-a522-9d2484a1607e",
                        "processor": {"values": {}, "name": "StartEndEvents"},
                    },
                ],
                "links": [
                    {"source": "a911d6de-48a9-4898-8d4f-c123efc84498", "target": "81e5ead2-c0ed-43c5-a522-9d2484a1607e"}
                ],
            }
        )

        export_data = graph.export(payload={})

        assert 1 == len(export_data["links"])
        del export_data["links"]
        del export_data["nodes"][0]["pk"]
        del export_data["nodes"][1]["pk"]

        assert {
            "directed": True,
            "nodes": [
                {"name": "SourceNode"},
                {"name": "EventsNode", "processor": {"values": {}, "name": "StartEndEvents"}},
            ],
        } == export_data

    """
    {'directed': True, 'nodes': [
    {'name': 'SourceNode', 'pk': 'a911d6de-48a9-4898-8d4f-c123efc84498'},
    {'name': 'EventsNode', 'pk': '81e5ead2-c0ed-43c5-a522-9d2484a1607e', 'processor': {'values': {}, 'name': 'StartEndEvents'}}], 'links': [{'source': 'a911d6de-48a9-4898-8d4f-c123efc84498', 'target': '81e5ead2-c0ed-43c5-a522-9d2484a1607e'}]}
    """
