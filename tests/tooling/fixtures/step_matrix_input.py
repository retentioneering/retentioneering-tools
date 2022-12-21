import os

import pandas as pd
import pytest

from src import datasets
from src.data_processors_lib import (
    FilterEvents,
    FilterEventsParams,
    StartEndEvents,
    StartEndEventsParams,
)
from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.graph.p_graph import EventsNode, PGraph

FLOAT_PRECISION = 3


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    return df


@pytest.fixture
def stream_simple_shop():
    def remove_start(df, schema):
        return df[schema.event_name] != "path_start"

    test_stream = datasets.load_simple_shop()
    graph = PGraph(source_stream=test_stream)
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams(**{})))
    node2 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_start)))

    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])

    stream = graph.combine(node=node2)
    return stream


@pytest.fixture
def test_stream_end_path():
    df = read_test_data("test_stream_end_path.csv")
    source_stream = Eventstream(df)
    return source_stream


@pytest.fixture
def test_stream():
    df = read_test_data("test_stream.csv")
    source_stream = Eventstream(df)
    return source_stream


@pytest.fixture
def test_weight_col():
    df = read_test_data("test_weight_col.csv")

    raw_data_schema = RawDataSchema(
        event_name="event",
        event_timestamp="timestamp",
        user_id="user_id",
        custom_cols=[{"custom_col": "session_id", "raw_data_col": "session_id"}],
    )
    source_stream = Eventstream(
        schema=EventstreamSchema(
            custom_cols=["session_id"], event_name="event", event_timestamp="timestamp", user_id="user_id"
        ),
        raw_data_schema=raw_data_schema,
        raw_data=df,
    )
    return source_stream
