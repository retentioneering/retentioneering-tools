import pandas as pd
import pytest

from src import datasets
from src.data_processors_lib.rete import (
    FilterEvents,
    FilterEventsParams,
    StartEndEvents,
    StartEndEventsParams,
)
from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph


@pytest.fixture
def stream_simple():
    source_df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event1", "2022-01-01 00:03:00"],
            [1, "event3", "2022-01-01 00:03:30"],
            [1, "event1", "2022-01-01 00:04:00"],
            [1, "event3", "2022-01-01 00:04:30"],
            [1, "event1", "2022-01-01 00:05:00"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event2", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:05"],
            [3, "event1", "2022-01-02 00:01:10"],
            [3, "event1", "2022-01-02 00:02:05"],
            [3, "event4", "2022-01-02 00:03:05"],
            [4, "event1", "2022-01-02 00:01:10"],
            [4, "event1", "2022-01-02 00:02:05"],
            [4, "event1", "2022-01-02 00:03:05"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    source_stream = Eventstream(
        raw_data=source_df,
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )

    return source_stream


@pytest.fixture
def stream_simple_shop():
    def remove_start(df, schema):
        return df["event_name"] != "path_start"

    test_stream = datasets.load_simple_shop()
    graph = PGraph(source_stream=test_stream)
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams(**{})))
    node2 = EventsNode(FilterEvents(params=FilterEventsParams(filter=remove_start)))

    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])

    stream = graph.combine(node=node2)
    return stream
