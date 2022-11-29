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


@pytest.fixture
def test_stream_end_path():
    df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event3", "2022-01-01 00:04:30"],
            [1, "path_end", "2022-01-01 00:04:30"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event2", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:05"],
            [2, "path_end", "2022-01-02 00:01:05"],
            [3, "event1", "2019-12-15 02:25:03"],
            [3, "event1", "2020-02-21 14:19:53"],
            [3, "event1", "2020-02-21 14:23:25"],
            [3, "event5", "2020-02-21 14:26:28"],
            [3, "event1", "2022-01-02 00:01:10"],
            [3, "event1", "2022-01-02 00:02:05"],
            [3, "event4", "2022-01-02 00:03:05"],
            [3, "path_end", "2022-01-02 00:03:05"],
            [4, "event1", "2019-11-21 16:19:55"],
            [4, "event2", "2020-02-28 07:59:40"],
            [4, "event1", "2020-01-29 09:10:04"],
            [4, "event1", "2022-01-02 00:03:05"],
            [4, "path_end", "2022-01-02 00:03:05"],
            [5, "event1", "2020-04-17 12:18:49"],
            [5, "event1", "2020-04-17 12:18:50"],
            [5, "path_end", "2020-04-17 12:18:50"],
            [6, "event1", "2019-11-12 10:02:06"],
            [6, "event2", "2019-11-12 10:02:34"],
            [6, "event3", "2019-11-12 10:02:36"],
            [6, "event4", "2019-11-12 10:02:39"],
            [6, "event5", "2019-11-12 10:03:04"],
            [6, "event1", "2019-11-12 10:03:06"],
            [6, "event2", "2019-11-12 10:03:34"],
            [6, "event3", "2019-11-12 10:03:38"],
            [6, "event4", "2019-11-12 10:03:39"],
            [6, "event5", "2019-11-12 10:04:40"],
            [6, "path_end", "2019-11-12 10:04:40"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    source_stream = Eventstream(
        raw_data=df,
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )

    return source_stream


@pytest.fixture
def test_stream():
    df = pd.DataFrame(
        [
            [1, "event1", "2022-01-01 00:01:00"],
            [1, "event2", "2022-01-01 00:01:02"],
            [1, "event1", "2022-01-01 00:02:00"],
            [1, "event3", "2022-01-01 00:04:30"],
            [2, "event1", "2022-01-02 00:00:00"],
            [2, "event2", "2022-01-02 00:00:05"],
            [2, "event2", "2022-01-02 00:01:05"],
            [3, "event1", "2022-01-02 00:01:10"],
            [3, "event1", "2022-01-02 00:02:05"],
            [3, "event4", "2022-01-02 00:03:05"],
            [4, "event1", "2022-01-02 00:03:05"],
            [5, "event1", "2020-04-17 12:18:49"],
            [6, "event1", "2019-11-12 10:03:06"],
            [6, "event2", "2019-11-12 10:03:36"],
            [6, "event3", "2019-11-12 10:03:36"],
            [6, "event4", "2019-11-12 10:03:39"],
            [6, "event5", "2019-11-12 10:03:40"],
            [5, "event1", "2020-04-17 12:18:50"],
            [6, "event1", "2019-11-12 10:03:07"],
            [6, "event2", "2019-11-12 10:03:34"],
            [6, "event3", "2019-11-12 10:03:38"],
            [6, "event4", "2019-11-12 10:03:41"],
            [6, "event5", "2019-11-12 10:03:42"],
            [4, "event1", "2019-11-21 16:19:55"],
            [4, "event2", "2020-02-28 07:59:40"],
            [4, "event1", "2020-01-29 09:10:04"],
            [3, "event1", "2019-12-15 02:25:03"],
            [3, "event5", "2020-02-21 14:26:28"],
            [3, "event1", "2020-02-21 14:19:53"],
            [3, "event1", "2020-02-21 14:23:25"],
        ],
        columns=["user_id", "event", "timestamp"],
    )

    source_stream = Eventstream(
        raw_data=df,
        raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
        schema=EventstreamSchema(),
    )

    return source_stream
