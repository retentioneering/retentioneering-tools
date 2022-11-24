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
CUSTOM_PREСISION = 3


def read_test_data(filename):
    # путь к папке с правильными ответами
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "step_matrix_test_data", filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


@pytest.fixture
def stream():
    def remove_start(df, schema):  # зачем?
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
    # правильные ответы в отдельном файле
    test_is_correct = result.compare(result_correct).shape == (0, 0)
    return test_is_correct


## отдельная функция для Eventstream
def make_eventstream(df):
    # create data schema
    raw_data_schema = RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id")

    # create source eventstream
    es = Eventstream(raw_data=df, raw_data_schema=raw_data_schema, schema=EventstreamSchema())
    return es


### тестовые датасеты
def test_data():
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
            [6, "event2", "2019-11-12 10:03:34"],
            [6, "event3", "2019-11-12 10:03:36"],
            [6, "event4", "2019-11-12 10:03:39"],
            [6, "event5", "2019-11-12 10:03:39"],
            [5, "event1", "2020-04-17 12:18:49"],
            [6, "event1", "2019-11-12 10:03:06"],
            [6, "event2", "2019-11-12 10:03:34"],
            [6, "event3", "2019-11-12 10:03:38"],
            [6, "event4", "2019-11-12 10:03:39"],
            [6, "event5", "2019-11-12 10:03:40"],
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
    return df


class TestStepMatrix:
    def test_step_matrix_simple(self):
        # тестовая таблица на вход
        source_df = test_data()
        # таблица с правильными ответами
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.167],
                [0.0, 0.0, 0.0, 0.167, 0.0],
            ],
            index=["event1", "event2", "event3", "event5"],
            columns=[1, 2, 3, 4, 5],
        )
        source_stream = Eventstream(
            raw_data=source_df,  # тестовая таблица
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )
        sm = StepMatrix(eventstream=source_stream, max_steps=5)
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    # тесты с таблицами из файлов, источник - simpleshop, в файле - готовая матрица
    def test_step_matrix__basic(self, stream):
        assert run_test(stream, "01_basic.csv")
        # самый обычный StepMatrix, без параметров

    def test_step_matrix__one_step(self, stream):
        assert run_test(stream, "02_one_step.csv", max_steps=1)
        # эти с параметром max_steps

    def test_step_matrix__100_steps(self, stream):
        assert run_test(stream, "03_100_steps.csv", max_steps=100, precision=3)
        # а теперь идем дальше

    def test_step_matrix__max_steps(self):  # 7 шагов
        # тестовая таблица (eventstream) на вход
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        # таблица с правильными ответами
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167, 0.0],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.167, 0.167, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.333],
            ],
            index=["event1", "event2", "event3", "event5", "event4"],
            columns=[1, 2, 3, 4, 5, 6, 7],
        )

        sm = StepMatrix(eventstream=source_stream, max_steps=7)
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__one_step(self):
        # тестовая таблица (eventstream) на вход
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame([[1.0]], index=["event1"], columns=[1])

        sm = StepMatrix(eventstream=source_stream, max_steps=1)
        result = sm._get_plot_data()[0]
        assert result.compare(correct_result).shape == (0, 0)

    # thresholds
    # def test_step_matrix__thresh(self):
    # тестовая таблица (eventstream) на вход
    #    source_df = test_data()
    #    source_stream = make_eventstream(source_df)
    #    correct_result = pd.DataFrame(     [
    #        [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
    #       [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
    #      [0.0, 0.0, 0.0, 0.334, 0.167, 0.167]

    # ], index=['event1', 'event2', 'THRESHOLDED_2'], columns=[1, 2, 3, 4, 5, 6],
    # )
    # sm = StepMatrix(eventstream=source_stream, max_steps=6, thresh=0.5)
    # result = sm._get_plot_data()[0].round(CUSTOM_PRECISION)
    # assert result.compare(correct_result).shape == (0, 0)

    #    def test_step_matrix__thresh_1(self):
    #     #      # тестовая таблица (eventstream) на вход
    #     #     source_df = test_data()
    #     #     source_stream = make_eventstream(source_df)
    #     #     correct_result = pd.DataFrame(
    #     #         [
    #     #             [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
    #     #             [0.0, 0.333, 0.5, 0.501, 0.167, 0.167],
    #     #
    #
    #     #         ], index=['event1', 'THRESHOLDED_3'], columns=[1, 2, 3, 4, 5, 6]
    #     #     )
    #     #     sm = StepMatrix(eventstream=source_stream, max_steps=6, thresh=1.0)
    #     #     result = sm._get_plot_data()[0].round(CUSTOM_PRECISION)
    #     #     assert result.compare(correct_result).shape == (0, 0)
    ## Targets
    def test_step_matrix__target(self):
        # тестовая таблица (eventstream) на вход
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167, 0.0],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.167, 0.167, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.333],
            ],
            index=["event1", "event2", "event3", "event5", "event4"],
            columns=[1, 2, 3, 4, 5, 6, 7],
        )
        sm = StepMatrix(eventstream=source_stream, max_steps=7, targets=["event5"])
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__target_thresh(self):
        # threshhold = 0.5
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.334, 0.167, 0.167],
            ],
            index=["event1", "event2", "THRESHOLDED_2"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        sm = StepMatrix(eventstream=source_stream, max_steps=6, targets=["payment_card", "payment_cash"], thresh=0.5)
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__grouping_targets(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.334, 0.167, 0.167],
            ],
            index=["event1", "event2", "THRESHOLDED_2"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        sm = StepMatrix(eventstream=source_stream, max_steps=6, targets=[["event3", "event5"]], thresh=0.5)
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    ## Аккумулирование
    def test_step_matrix__accumulated_only(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.334, 0.167, 0.167],
            ],
            index=["event1", "event2", "THRESHOLDED_2"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        sm = StepMatrix(
            eventstream=source_stream, max_steps=11, targets=["payment_card"], thresh=0.5, accumulated="only"
        )
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__accumulated_both(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.333, 0.167, 0.167],
            ],
            index=["event1", "event2", "THRESHOLDED_2"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        sm = StepMatrix(eventstream=source_stream, max_steps=6, targets=["event4"], thresh=0.5, accumulated="both")
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    # центрирование (с таргетом и без)
    def test_step_matrix__centered(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.5, 0.5, 0.5, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0],
                [0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
            ],
            index=(["event2", "event1", "event3", "event4", "event5"]),
            columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            ## в новой версии индексы 12345678 вместо -1,-2,-3,-4, 0, 1...
            ## и строки меняются местами, выстраиваются в порядке возникновения событий
        )
        sm = StepMatrix(
            eventstream=source_stream, max_steps=10, centered={"event": "event5", "left_gap": 4, "occurrence": 1}
        )
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__centered_target(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
                [0.5, 1.0, 1.0, 1.0, 0.0, 1.0, 0.5, 0.5, 0.0, 0.0],
            ],
            index=(["event5", "THRESHOLDED_4"]),
            columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        )
        sm = StepMatrix(
            eventstream=source_stream,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            thresh=0.6,
        )
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    # Custom events sorting
    def test_step_matrix__events_sorting(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.167, 0.0, 0.0],  # event5
                [0.0, 0.0, 0.0, 0.167, 0.167, 0.167],  # event3
                [0.0, 0.333, 0.50, 0.167, 0.0, 0.0],  # event2
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],  # event1
                # new order
            ],
            index=(["event5", "event3", "event2", "event1"]),
            columns=[1, 2, 3, 4, 5, 6],
        )
        new_order = ["event5", "event3", "event2", "event1"]
        sm = StepMatrix(eventstream=source_stream, sorting=new_order, max_steps=6)
        result = sm._get_plot_data()[0].round(CUSTOM_PREСISION)
        assert result.compare(correct_result).shape == (0, 0)

    ## Differential step matrix

    def test_step_matrix__differential(self):
        source_df = test_data()
        source_stream = make_eventstream(source_df)
        g1 = set(source_df[source_df["event"] == "event5"]["user_id"])
        g2 = set(source_df["user_id"]) - g1
        correct_result = pd.DataFrame(
            [
                [0, 1.0, 0.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            ],
            index=(["event1", "event2", "event3", "event4", "event5"]),
            columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        )
        sm = StepMatrix(
            eventstream=source_stream,
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )
        result = sm._get_plot_data()[0]
        assert result.compare(correct_result).shape == (0, 0)
