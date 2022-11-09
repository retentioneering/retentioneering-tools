import os

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
from src.tooling.step_matrix import StepMatrix

FLOAT_PRECISION = 6


def read_test_data(filename):
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "step_matrix_test_data", filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


@pytest.fixture
def stream():
    def remove_start(df, schema):
        return df["event_name"] != "start"

    test_stream = datasets.load_simple_shop()
    graph = PGraph(source_stream=test_stream)
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams(**{})))
    node2 = EventsNode(FilterEvents(params=FilterEventsParams(filter=remove_start)))

    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])

    stream = graph.combine(node=node2)
    return stream


def run_test(stream, filename, **kwargs):
    sm = StepMatrix(eventstream=stream, **kwargs)
    result, _, _, _ = sm._get_plot_data()
    result = result.round(FLOAT_PRECISION)
    result_correct = read_test_data(filename)
    test_is_correct = result.compare(result_correct).shape == (0, 0)
    return test_is_correct


class TestStepMatrix:
    def test_step_matrix_simple(self):
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

        correct_result = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )

        source_stream = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )

        sm = StepMatrix(eventstream=source_stream, max_steps=5)
        result, _, _, _ = sm._get_plot_data()
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__basic(self, stream):
        assert run_test(stream, "01_basic.csv")

    def test_step_matrix__one_step(self, stream):
        assert run_test(stream, "02_one_step.csv", max_steps=1)

    def test_step_matrix__100_steps(self, stream):
        assert run_test(stream, "03_100_steps.csv", max_steps=100, precision=3)
