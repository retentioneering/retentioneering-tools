import json

import pandas as pd

from src.data_processors_lib.rete import StartEndEventsParams, StartEndEvents
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
        node = EventsNode(
            processor=StartEndEvents(params=StartEndEventsParams(**{}))
        )
        graph.add_node(node=node, parents=[graph.root])

        export_data = graph.export()
        del export_data["links"][0]['source'].pk
        del export_data["links"][0]['target'].pk
        del export_data["nodes"][0]['pk']
        del export_data["nodes"][1]['pk']
        print(export_data)
        assert json.dumps({'directed': True,
                'links': [{'source': {'name': 'SourceNode'},
                           'target': {'name': 'EventsNode'}}],
                'nodes': [{'name': 'SourceNode'},
                          {'name': 'EventsNode',
                           'processor': {'name': 'StartEndEvents', 'values': {}}}]}) == json.dumps(export_data)

    """
    {'directed': True,
                'links': [{'source': {'name': 'SourceNode', 'pk': '47f68185-a4a4-4493-9dcd-d64116190d95'},
                           'target': {'name': 'EventsNode', 'pk': '5b1a9b1b-886d-4334-abbb-6f903581f280'}}],
                'nodes': [{'name': 'SourceNode', 'pk': '47f68185-a4a4-4493-9dcd-d64116190d95'},
                          {'name': 'EventsNode',
                           'pk': '5b1a9b1b-886d-4334-abbb-6f903581f280',
                           'processor': {'name': 'StartEndEvents', 'values': {}}}]}
                           """