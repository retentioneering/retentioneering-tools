from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import StartEndEvents, StartEndEventsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.graph.p_graph import PGraph, EventsNode


class TestStartEndEvents:
    def test_start_end(self):
        source_df = pd.DataFrame([
            [1, 'pageview', 'raw', '2021-10-26 12:00'],
            [1, 'cart_btn_click', 'raw', '2021-10-26 12:02'],
            [1, 'plus_icon_click', 'raw', '2021-10-26 12:04'],
        ], columns=['user_id', 'event_name', 'event_type', 'event_timestamp']
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        events = StartEndEvents(StartEndEventsParams(**{}))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names: list[str] = result_df[result.schema.event_name].to_list()
        assert ["start", "end"] == events_names

    def test_start_end_2(self):
        source_df = pd.DataFrame([
            [1, 'event1', '2022-01-01 00:00:00'],
            [1, 'event2', '2022-01-01 00:00:01'],
            [1, 'event3', '2022-01-01 00:00:02'],
            [2, 'event4', '2022-01-02 00:00:00'],
        ], columns=['user_id', 'event', 'timestamp']
        )

        stream = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name='event', event_timestamp='timestamp', user_id='user_id'
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        graph = PGraph(source_stream=stream)
        start_end_events = EventsNode(StartEndEvents(params=StartEndEventsParams(**{})))
        graph.add_node(node=start_end_events, parents=[graph.root])
        res = graph.combine(node=start_end_events).to_dataframe(show_deleted=True)

        assert res[res['user_id'] == 1].iloc[0]['event_name'] == 'start'
        assert res[res['user_id'] == 1].iloc[-1]['event_name'] == 'end'
        assert res[res['user_id'] == 2].iloc[0]['event_name'] == 'start'
        assert res[res['user_id'] == 2].iloc[-1]['event_name'] == 'end'

