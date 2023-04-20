from typing import Any

import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.step_matrix import StepMatrix
from tests.tooling.fixtures.step_matrix_corr import (
    accumulated_both_targets_plot_cor,
    accumulated_only_targets_plot_cor,
    centered_cor,
    centered_name_cor,
    centered_target_thresh_cor,
    centered_target_thresh_plot_cor,
    differential_cor,
    differential_name_cor,
    events_sorting_cor,
    max_steps_100_cor,
    max_steps_cor,
    max_steps_one_cor,
    path_end_cor,
    targets_grouping_cor,
    targets_plot_cor,
    targets_thresh_plot_cor,
    thresh_1_cor,
    thresh_cor,
    weight_col_cor,
)
from tests.tooling.fixtures.step_matrix_input import (
    stream_simple_shop,
    test_stream,
    test_stream_end_path,
    test_weight_col,
)

FLOAT_PRECISION = 3


def run_test(stream: EventstreamType, corr_fixture: pd.DataFrame, val: int, **kwargs: Any) -> bool:
    sm = StepMatrix(eventstream=stream)
    sm.fit(**kwargs)
    result = sm.values[val].round(FLOAT_PRECISION)

    return result.compare(corr_fixture).shape == (0, 0)


class TestStepMatrix:
    def test_step_matrix__max_steps(self, test_stream: EventstreamType, max_steps_cor: pd.DataFrame) -> None:
        assert run_test(test_stream, max_steps_cor, val=0, max_steps=5)

    def test_step_matrix__max_steps_100(
        self, stream_simple_shop: EventstreamType, max_steps_100_cor: pd.DataFrame
    ) -> None:
        assert run_test(stream_simple_shop, max_steps_100_cor, val=0, max_steps=100)

    def test_step_matrix__max_steps_one(self, test_stream: EventstreamType, max_steps_one_cor: pd.DataFrame) -> None:
        assert run_test(test_stream, max_steps_one_cor, val=0, max_steps=1)

    def test_step_matrix__thresh(self, test_stream: EventstreamType, thresh_cor: pd.DataFrame) -> None:
        assert run_test(test_stream, thresh_cor, val=0, max_steps=6, threshold=0.3)

    def test_step_matrix__thresh_1(self, test_stream: EventstreamType, thresh_1_cor: pd.DataFrame) -> None:
        assert run_test(test_stream, thresh_1_cor, val=0, max_steps=6, threshold=1.0)

    def test_step_matrix__targets_plot(self, test_stream: EventstreamType, targets_plot_cor: pd.DataFrame) -> None:
        assert run_test(test_stream, targets_plot_cor, val=1, max_steps=6, targets=["event3"])

    def test_step_matrix__targets_thresh_plot(
        self, test_stream: EventstreamType, targets_thresh_plot_cor: pd.DataFrame
    ) -> None:
        assert run_test(
            test_stream, targets_thresh_plot_cor, val=1, max_steps=6, targets=["event3", "event5"], threshold=0.5
        )

    def test_step_matrix__targets_grouping(
        self, test_stream: EventstreamType, targets_grouping_cor: pd.DataFrame
    ) -> None:
        correct_result = targets_grouping_cor
        sm = StepMatrix(eventstream=test_stream)
        sm.fit(max_steps=6, targets=[["event3", "event5"]])
        result = sm.targets_list
        assert correct_result == result

    def test_step_matrix__accumulated_only_targets_plot(
        self, test_stream: EventstreamType, accumulated_only_targets_plot_cor: pd.DataFrame
    ) -> None:
        assert run_test(
            test_stream,
            accumulated_only_targets_plot_cor,
            val=1,
            max_steps=10,
            targets=["event5"],
            accumulated="only",
        )

    def test_step_matrix__accumulated_both_targets_plot(
        self, test_stream: EventstreamType, accumulated_both_targets_plot_cor: pd.DataFrame
    ) -> None:
        assert run_test(
            test_stream,
            accumulated_both_targets_plot_cor,
            val=1,
            max_steps=10,
            targets=["event4", "event5"],
            accumulated="both",
        )

    def test_step_matrix__centered(self, test_stream: EventstreamType, centered_cor: pd.DataFrame) -> None:
        assert run_test(
            test_stream,
            centered_cor,
            val=0,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
        )

    def test_step_matrix__centered_target_thresh(
        self, test_stream: EventstreamType, centered_target_thresh_cor: pd.DataFrame
    ) -> None:
        assert run_test(
            test_stream,
            centered_target_thresh_cor,
            val=0,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            threshold=0.6,
        )

    def test_step_matrix__centered_target_thresh_plot(
        self, test_stream: EventstreamType, centered_target_thresh_plot_cor: pd.DataFrame
    ) -> None:
        assert run_test(
            test_stream,
            centered_target_thresh_plot_cor,
            val=1,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            threshold=0.6,
        )

    def test_step_matrix__centered_name(self, test_stream: EventstreamType, centered_name_cor: pd.DataFrame) -> None:
        correct_result = centered_name_cor
        sm = StepMatrix(eventstream=test_stream)
        sm.fit(max_steps=10, centered={"event": "event5", "left_gap": 4, "occurrence": 1}, threshold=0.6)
        result = sm.fraction_title
        assert correct_result == result

    def test_step_matrix__events_sorting(self, test_stream: EventstreamType, events_sorting_cor: pd.DataFrame) -> None:
        new_order = ["event5", "event3", "event2", "event1", "ENDED"]
        assert run_test(test_stream, events_sorting_cor, val=0, sorting=new_order, max_steps=6)

    def test_step_matrix__differential(self, test_stream: EventstreamType, differential_cor: pd.DataFrame):
        g1 = [3, 6]
        g2 = [1, 2, 5]

        assert run_test(
            test_stream,
            differential_cor,
            val=0,
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )

    def test_step_matrix__differential_name(
        self, test_stream: EventstreamType, differential_name_cor: pd.DataFrame
    ) -> None:
        g1 = [3, 6]
        g2 = [1, 2, 5]
        correct_result = differential_name_cor
        sm = StepMatrix(eventstream=test_stream)
        sm.fit(
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )
        result = sm.fraction_title
        assert correct_result == result

    def test_step_matrix__path_end(self, test_stream_end_path: EventstreamType, path_end_cor: pd.DataFrame) -> None:
        assert run_test(test_stream_end_path, path_end_cor, val=0, max_steps=5)

    def test_step_matrix__weight_col(self, test_weight_col: EventstreamType, weight_col_cor: pd.DataFrame) -> None:
        assert run_test(test_weight_col, weight_col_cor, val=0, max_steps=5, weight_col="session_id")
