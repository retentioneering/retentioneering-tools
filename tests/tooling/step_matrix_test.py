from src.tooling.step_matrix import StepMatrix
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


class TestStepMatrix:
    def test_step_matrix__max_steps(self, test_stream, max_steps_cor):
        correct_result = max_steps_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=5)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__max_steps_100(self, stream_simple_shop, max_steps_100_cor):
        sm = StepMatrix(eventstream=stream_simple_shop, max_steps=100)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        correct_result = max_steps_100_cor.round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__max_steps_one(self, test_stream, max_steps_one_cor):
        sm = StepMatrix(eventstream=test_stream, max_steps=1)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        correct_result = max_steps_one_cor
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__thresh(self, test_stream, thresh_cor):
        correct_result = thresh_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=6, thresh=0.3)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__thresh_1(self, test_stream, thresh_1_cor):
        correct_result = thresh_1_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=6, thresh=1.0)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__targets_plot(self, test_stream, targets_plot_cor):
        correct_result = targets_plot_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=6, targets=["event3"])
        sm.fit()
        result = sm.values[1].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__targets_thresh_plot(self, test_stream, targets_thresh_plot_cor):
        correct_result = targets_thresh_plot_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=6, targets=["event3", "event5"], thresh=0.5)
        sm.fit()
        result = sm.values[1].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__targets_grouping(self, test_stream, targets_grouping_cor):
        correct_result = targets_grouping_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=6, targets=[["event3", "event5"]])
        sm.fit()
        result = sm.targets_list
        assert correct_result == result

    def test_step_matrix__accumulated_only_targets_plot(self, test_stream, accumulated_only_targets_plot_cor):
        correct_result = accumulated_only_targets_plot_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=10, targets=["event5"], accumulated="only")
        sm.fit()
        result = sm.values[1].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__accumulated_both_targets_plot(self, test_stream, accumulated_both_targets_plot_cor):
        correct_result = accumulated_both_targets_plot_cor
        sm = StepMatrix(eventstream=test_stream, max_steps=10, targets=["event4", "event5"], accumulated="both")
        sm.fit()
        result = sm.values[1].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__centered(self, test_stream, centered_cor):
        correct_result = centered_cor
        sm = StepMatrix(
            eventstream=test_stream, max_steps=10, centered={"event": "event5", "left_gap": 4, "occurrence": 1}
        )
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__centered_target_thresh(self, test_stream, centered_target_thresh_cor):
        correct_result = centered_target_thresh_cor
        sm = StepMatrix(
            eventstream=test_stream,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            thresh=0.6,
        )
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__centered_target_thresh_plot(self, test_stream, centered_target_thresh_plot_cor):
        correct_result = centered_target_thresh_plot_cor
        sm = StepMatrix(
            eventstream=test_stream,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            thresh=0.6,
        )
        sm.fit()
        result = sm.values[1].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__centered_name(self, test_stream, centered_name_cor):
        correct_result = centered_name_cor
        sm = StepMatrix(
            eventstream=test_stream,
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            thresh=0.6,
        )
        sm.fit()
        result = sm.fraction_title
        assert correct_result == result

    def test_step_matrix__events_sorting(self, test_stream, events_sorting_cor):
        new_order = ["event5", "event3", "event2", "event1", "ENDED"]
        correct_result = events_sorting_cor
        sm = StepMatrix(eventstream=test_stream, sorting=new_order, max_steps=6)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__differential(self, test_stream, differential_cor):
        g1 = [3, 6]
        g2 = [1, 2, 5]
        correct_result = differential_cor
        sm = StepMatrix(
            eventstream=test_stream,
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__differential_name(self, test_stream, differential_name_cor):
        g1 = [3, 6]
        g2 = [1, 2, 5]
        correct_result = differential_name_cor
        sm = StepMatrix(
            eventstream=test_stream,
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )
        sm.fit()
        result = sm.fraction_title
        assert correct_result == result

    def test_step_matrix__path_end(self, test_stream_end_path, path_end_cor):
        correct_result = path_end_cor
        sm = StepMatrix(eventstream=test_stream_end_path, max_steps=5)
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__weight_col(self, test_weight_col, weight_col_cor):
        correct_result = weight_col_cor
        sm = StepMatrix(eventstream=test_weight_col, max_steps=5, weight_col=["session_id"])
        sm.fit()
        result = sm.values[0].round(FLOAT_PRECISION)
        assert result.compare(correct_result).shape == (0, 0)
