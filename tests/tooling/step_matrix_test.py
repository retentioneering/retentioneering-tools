import os

import pandas as pd

from src.tooling.step_matrix import StepMatrix
from tests.tooling.fixtures.step_matrix import stream_simple, stream_simple_shop

FLOAT_PRECISION = 6
CUSTOM_PREСISION = 3


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


def run_test(stream, filename, **kwargs):
    sm = StepMatrix(eventstream=stream, **kwargs)
    result, _, _, _ = sm._get_plot_data()
    result = result.round(FLOAT_PRECISION)
    result_correct = read_test_data(filename)
    test_is_correct = result.compare(result_correct).shape == (0, 0)
    return test_is_correct


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
    def test_step_matrix__simple(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5)
        result, _, _, _ = sm._get_plot_data()


class TestStepMatrix:
    def test_step_matrix_simple(self):
        source_df = test_data()
        correct_result = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )
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
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__simple_thresh(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5, thresh=0.3)
        result, _, _, _ = sm._get_plot_data()

        correct_result = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "THRESHOLDED_1"],
            columns=[1, 2, 3, 4, 5],
        )
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__simple_target(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5, targets=["event3"])
        result, targets_result, _, _ = sm._get_plot_data()
        result = pd.concat([result, targets_result])

        correct_result = pd.DataFrame(
            [
                [1.0, 0.5, 0.5, 0.25, 0.25],
                [0.0, 0.5, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            index=["event1", "event2", "event4", "event3"],
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

    def test_step_matrix__basic(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "01_basic.csv")

    def test_step_matrix__one_step(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "02_one_step.csv", max_steps=1)

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

    def test_step_matrix__100_steps(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "03_100_steps.csv", max_steps=100, precision=3)
