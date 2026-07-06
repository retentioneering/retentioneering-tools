import pandas as pd
import pytest
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PatternNoMatchError


class TestStepMatrix:
    def test__simple(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 5
        res = stream.step_sankey_data(max_steps=max_steps)[0]

        expected = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 2 / 3, 0.0, 1 / 3, 1 / 3, 0.0],
                [0.0, 1 / 3, 1.0, 1 / 3, 1 / 3, 1 / 3],
                [0.0, 0.0, 0.0, 1 / 3, 0.0, 1 / 3],
                [0.0, 0.0, 0.0, 0.0, 1 / 3, 1 / 3],
            ],
            index=["path_start", "A", "B", "C", "path_end"],
            columns=range(max_steps + 1),
        )
        expected.index.name = "event"
        expected.columns.name = "step"

        pd.testing.assert_frame_equal(res, expected)

    def test__diff(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(
            df, {"path_cols": ["session_id"], "segment_cols": ["country"]}
        )
        max_steps = 5
        res = stream.step_sankey_data(
            max_steps=max_steps, diff=("country", "US", "UK"), path_col="session_id"
        )[0][0]

        expected = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 1 / 2, -1 / 2.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 1 / 2, -1 / 2],
                [0.0, 0.0, 0.0, -1 / 2, 0.0, 1 / 2],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            index=["path_start", "A", "B", "C", "path_end"],
            columns=range(max_steps + 1),
        )
        expected.index.name = "event"
        expected.columns.name = "step"

        pd.testing.assert_frame_equal(res, expected)

    def test__diff_with_rest_pools_all_other_segment_values(self) -> None:
        """diff value2="<REST>" must pool every other segment value, not just one."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "session_1", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "session_1", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "session_2", "UK"],
                ["user_2", "B", "2020-01-01 00:01:00", "session_2", "UK"],
                ["user_2", "C", "2020-01-01 00:02:00", "session_2", "UK"],
                ["user_3", "A", "2020-01-01 00:00:00", "session_3", "DE"],
            ],
            columns=["user_id", "event", "timestamp", "session_id", "country"],
        )
        stream = Eventstream(
            df, {"path_cols": ["session_id"], "segment_cols": ["country"]}
        )
        max_steps = 3
        diff_sms, sms1, sms2 = stream.step_sankey_data(
            max_steps=max_steps, diff=("country", "US", "<REST>"), path_col="session_id"
        )

        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")
        columns = pd.Index(range(max_steps + 1), name="step")

        # group1 = US only (session_1)
        expected_g1 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ],
            index=index,
            columns=columns,
        )

        # group2 = <REST> = UK + DE pooled (session_2, session_3)
        expected_g2 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 0.5, 0.0],
                [0.0, 0.0, 0.0, 0.5],
                [0.0, 0.0, 0.5, 0.5],
            ],
            index=index,
            columns=columns,
        )

        pd.testing.assert_frame_equal(sms1[0], expected_g1)
        pd.testing.assert_frame_equal(sms2[0], expected_g2)
        pd.testing.assert_frame_equal(diff_sms[0], expected_g2 - expected_g1)

    def test__path_end_session_id(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        schema = {"path_cols": ["user_id", "session_id"]}
        stream = Eventstream(df, schema)
        max_steps = 5
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern=".*->path_end", path_col="session_id"
        )[0]

        expected = pd.DataFrame(
            [
                [2 / 4, 2 / 4, 0.0, 0.0, 0.0, 0.0],
                [0.0, 1 / 4, 1 / 4, 0.0, 0.0, 0.0],
                [2 / 4, 0.0, 3 / 4, 3 / 4, 1 / 2, 0.0],
                [0.0, 1 / 4, 0.0, 1 / 4, 1 / 2, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
            ],
            index=["path_start", "A", "B", "C", "path_end"],
            columns=range(-max_steps, 1),
        )
        expected.index.name = "event"
        expected.columns.name = "step"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_pattern_1(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern="path_start->.*->C"
        )

        expected_0 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0],
                [0.0, 1.0, 0.0],
                [0.0, 0.0, 1.0],
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ],
            index=["path_start", "A", "B", "C", "path_end"],
            columns=list(range(max_steps + 1)),
        )

        expected_1 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0, 0.5, 0.0],
                [0.0, 1.0, 0.0, 0.0, 0.5],
                [0.0, 0.0, 1.0, 0.5, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.5],
            ],
            index=["path_start", "A", "B", "C", "path_end"],
            columns=range(-max_steps, max_steps + 1),
        )

        expected_0.index.name = "event"
        expected_1.index.name = "event"
        expected_0.columns.name = "step"
        expected_1.columns.name = "step"

        pd.testing.assert_frame_equal(res[0], expected_0)
        pd.testing.assert_frame_equal(res[1], expected_1)

    def test__path_pattern_2(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 3
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern="path_start->.*->path_end"
        )

        expected_0 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 2 / 3, 0.0, 1 / 3],
                [0.0, 1 / 3, 1.0, 1 / 3],
                [0.0, 0.0, 0.0, 1 / 3],
                [0.0, 0.0, 0.0, 0.0],
            ],
            index=pd.Index(["path_start", "A", "B", "C", "path_end"], name="event"),
            columns=pd.Index(range(max_steps + 1), name="step"),
        )

        expected_1 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0],
                [1.0, 2 / 3, 2 / 3, 0.0],
                [0.0, 1 / 3, 1 / 3, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ],
            index=pd.Index(["path_start", "A", "B", "C", "path_end"], name="event"),
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected_0)
        pd.testing.assert_frame_equal(res[1], expected_1)

    def test__path_pattern_3(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern="path_start->A->.*->C->.*->path_end"
        )

        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected_0 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.5],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0, 0.5],
                [0.0, 0.0, 0.0, 0.0],
            ],
            index=index,
            columns=pd.Index(range(max_steps + 1 + 1), name="step"),
        )

        expected_1 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0, 0.5, 0.0],
                [0.0, 1.0, 0.0, 0.0, 0.5],
                [0.0, 0.0, 1.0, 0.5, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.5],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, max_steps + 1), name="step"),
        )

        expected_2 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [0.5, 0.5, 0.0],
                [0.5, 0.5, 0.0],
                [0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected_0)
        pd.testing.assert_frame_equal(res[1], expected_1)
        pd.testing.assert_frame_equal(res[2], expected_2)

    def test__path_pattern_4(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern="path_start->.*->B->B->.*->path_end"
        )
        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected_0 = pd.DataFrame(
            [
                [1.0, 0.0, 0.0],
                [0.0, 0.5, 0.0],
                [0.0, 0.5, 1.0],
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ],
            index=index,
            columns=pd.Index(range(max_steps + 1), name="step"),
        )

        expected_1 = pd.DataFrame(
            [
                [0.5, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.5, 0.0, 1.0, 1.0, 1.0, 0.0],
                [0.0, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-2, 4), name="step"),
        )

        expected_2 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [1.0, 1.0, 0.0],
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected_0)
        pd.testing.assert_frame_equal(res[1], expected_1)
        pd.testing.assert_frame_equal(res[2], expected_2)

    def test__path_pattern_5(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(
            max_steps=max_steps, path_pattern="B->B->.*->path_end"
        )
        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected_0 = pd.DataFrame(
            [
                [0.5, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.5, 0.0, 1.0, 1.0, 1.0, 0.0],
                [0.0, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-2, 4), name="step"),
        )

        expected_1 = pd.DataFrame(
            [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [1.0, 1.0, 0.0],
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected_0)
        pd.testing.assert_frame_equal(res[1], expected_1)

    def test__path_pattern_6(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(max_steps=max_steps, path_pattern="B->B")
        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected_0 = pd.DataFrame(
            [
                [0.5, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.5, 0.0, 1.0, 1.0, 1.0, 0.0],
                [0.0, 0.5, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-2, 4), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected_0)

    def test__path_pattern_7(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(max_steps=max_steps, path_pattern=".*->path_end")
        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected = pd.DataFrame(
            [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [2 / 3, 2 / 3, 0.0],
                [1 / 3, 1 / 3, 0.0],
                [0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected)

    def test__path_pattern_8(self, fx_read_csv):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        res = stream.step_sankey_data(max_steps=max_steps, path_pattern="path_end")
        index = pd.Index(["path_start", "A", "B", "C", "path_end"], name="event")

        expected = pd.DataFrame(
            [
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [2 / 3, 2 / 3, 0.0],
                [1 / 3, 1 / 3, 0.0],
                [0.0, 0.0, 1.0],
            ],
            index=index,
            columns=pd.Index(range(-max_steps, 1), name="step"),
        )

        pd.testing.assert_frame_equal(res[0], expected)

    def test__path_pattern_empty_raises_error(self, fx_read_csv):
        """Test that PatternNoMatchError is raised when path_pattern matches no paths (regular case)"""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        # Use a pattern that will match no paths
        non_matching_pattern = "path_start->X->Y->Z->path_end"

        with pytest.raises(PatternNoMatchError) as exc_info:
            stream.step_sankey_data(
                max_steps=max_steps, path_pattern=non_matching_pattern
            )

        assert exc_info.value.error_code == "PATTERN_NO_MATCH"
        assert non_matching_pattern in exc_info.value.message

    def test__path_pattern_empty_raises_error_with_diff(self, fx_read_csv):
        """Test that PatternNoMatchError is raised when path_pattern matches no paths (diff case)"""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(
            df, {"path_cols": ["session_id"], "segment_cols": ["country"]}
        )
        max_steps = 2
        # Use a pattern that will match no paths
        non_matching_pattern = "path_start->X->Y->Z->path_end"

        with pytest.raises(PatternNoMatchError) as exc_info:
            stream.step_sankey_data(
                max_steps=max_steps,
                path_pattern=non_matching_pattern,
                diff=("country", "US", "UK"),
                path_col="session_id",
            )

        assert exc_info.value.error_code == "PATTERN_NO_MATCH"
        assert non_matching_pattern in exc_info.value.message


class TestStepMatrixDataAlias:
    def test__step_matrix_data_matches_step_sankey_data(self):
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00"],
                ["user_1", "B", "2020-01-01 00:01:00"],
                ["user_2", "A", "2020-01-01 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)
        via_sankey = stream.step_sankey_data(max_steps=3)
        via_matrix = stream.step_matrix_data(max_steps=3)
        assert len(via_sankey) == len(via_matrix)
        for a, b in zip(via_sankey, via_matrix):
            pd.testing.assert_frame_equal(a, b)
