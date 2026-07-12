import pandas as pd
import pytest
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError, PatternNoMatchError


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
                [0.0, 0.0, 0.0, -1 / 2, 1 / 2.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, -1 / 2, 1 / 2],
                [0.0, 0.0, 0.0, 1 / 2, 0.0, -1 / 2],
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
        pd.testing.assert_frame_equal(diff_sms[0], expected_g1 - expected_g2)

    def test__diff_with_rest_and_no_complement_raises(self) -> None:
        """diff value2="<REST>" must raise a clear error, not a raw DuckDB exception,
        when the chosen segment value has no complementary values."""
        from retentioneering.exceptions import DiffConfigError

        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "session_1", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "session_1", "US"],
            ],
            columns=["user_id", "event", "timestamp", "session_id", "country"],
        )
        stream = Eventstream(
            df, {"path_cols": ["session_id"], "segment_cols": ["country"]}
        )

        with pytest.raises(DiffConfigError):
            stream.step_sankey_data(
                max_steps=3, diff=("country", "US", "<REST>"), path_col="session_id"
            )

    def test__diff_with_path_pattern_uses_group1_minus_group2(self) -> None:
        """Regression guard: the combined diff matrix must be group1 - group2
        whether or not path_pattern is set. _process_pattern_matrix previously
        computed group2 - group1 here, an inverted sign relative to the
        no-pattern path (_process_diff_matrix)."""
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-01 00:01:00", "US"],
                ["user_2", "A", "2020-01-01 00:00:00", "US"],
                ["user_2", "B", "2020-01-01 00:01:00", "US"],
                ["user_3", "A", "2020-01-01 00:00:00", "UK"],
            ],
            columns=["user_id", "event", "timestamp", "country"],
        )
        stream = Eventstream(df, {"segment_cols": ["country"]})

        diff_sms, sms1, sms2 = stream.step_sankey_data(
            max_steps=3, diff=("country", "US", "UK"), path_pattern="A"
        )

        assert len(diff_sms) > 0
        for i in range(len(diff_sms)):
            pd.testing.assert_frame_equal(diff_sms[i], sms1[i] - sms2[i])

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

    def test__path_col_override_finer_grain_preserves_chronological_order(self):
        # path_cols must be coarsest-first (validated at Eventstream
        # construction): user_id then session_id. Overriding to session_id (a
        # valid, finer declared path_col) must still reflect real event order.
        df = pd.DataFrame(
            [
                ["U1", "S1", "A", "2024-01-01 10:00:00"],
                ["U1", "S1", "B", "2024-01-01 10:01:00"],
                ["U1", "S2", "C", "2024-01-01 10:02:00"],
                ["U1", "S2", "D", "2024-01-01 10:03:00"],
            ],
            columns=["user_id", "session_id", "event", "timestamp"],
        )
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        (res,) = stream.step_sankey_data(max_steps=4, path_col="session_id")

        # session S1: path_start -> A -> B -> path_end
        # session S2: path_start -> C -> D -> path_end
        expected = pd.DataFrame(
            [
                [1.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.5, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.5, 0.0, 0.0],
                [0.0, 0.5, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.5, 0.0, 0.0],
                [0.0, 0.0, 0.0, 1.0, 1.0],
            ],
            index=["path_start", "A", "B", "C", "D", "path_end"],
            columns=range(5),
        )
        expected.index.name = "event"
        expected.columns.name = "step"

        pd.testing.assert_frame_equal(res, expected)

    def test__path_col_override_rejects_undeclared_column(self):
        df = pd.DataFrame(
            [
                ["U1", "A", "2024-01-01 10:00:00"],
                ["U1", "B", "2024-01-01 10:01:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df, {"path_cols": ["user_id"]})
        with pytest.raises(InvalidParameterError):
            stream.step_sankey_data(max_steps=4, path_col="not_a_path_col")

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

    def test__path_pattern_9_redundant_wildcard_centers_anchor(self, fx_read_csv):
        """Regression test: wrapping the anchor in a redundant leading/trailing
        '.*' must still center it at column 0, not column -1."""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2

        with pytest.warns(UserWarning, match="redundant"):
            res = stream.step_sankey_data(max_steps=max_steps, path_pattern=".*->B->.*")

        assert res[0].loc["B", 0] == 1.0
        assert res[0].loc["B", -1] == 0.0

    def test__path_pattern_10_redundant_wildcard_matches_bare_pattern(
        self, fx_read_csv
    ):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2

        with pytest.warns(UserWarning, match="redundant"):
            wrapped = stream.step_sankey_data(
                max_steps=max_steps, path_pattern=".*->B->.*"
            )
        bare = stream.step_sankey_data(max_steps=max_steps, path_pattern="B")

        pd.testing.assert_frame_equal(wrapped[0], bare[0])

    def test__path_pattern_no_warning_without_redundant_wildcard(
        self, fx_read_csv, recwarn
    ):
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        stream.step_sankey_data(max_steps=2, path_pattern="path_start->.*->C")

        assert len(recwarn) == 0

    def test__path_pattern_empty_raises_error(self, fx_read_csv):
        """Test that PatternNoMatchError is raised when path_pattern matches no paths (regular case)"""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)
        max_steps = 2
        # A pattern of real events (A/B/C) that never occurs adjacently in the
        # fixture - legitimately zero matches, as opposed to a typo.
        non_matching_pattern = "path_start->A->A->A->path_end"

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
        # A pattern of real events (A/B/C) that never occurs adjacently in the
        # fixture - legitimately zero matches, as opposed to a typo.
        non_matching_pattern = "path_start->A->A->A->path_end"

        with pytest.raises(PatternNoMatchError) as exc_info:
            stream.step_sankey_data(
                max_steps=max_steps,
                path_pattern=non_matching_pattern,
                diff=("country", "US", "UK"),
                path_col="session_id",
            )

        assert exc_info.value.error_code == "PATTERN_NO_MATCH"
        assert non_matching_pattern in exc_info.value.message

    def test__path_pattern_typo_raises_invalid_parameter_error(self, fx_read_csv):
        """A typoed event in path_pattern must fail loudly with a specific
        'unknown event' error, not the generic PatternNoMatchError."""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)

        with pytest.raises(InvalidParameterError) as exc_info:
            stream.step_sankey_data(max_steps=2, path_pattern="path_start->X->path_end")

        assert "X" in exc_info.value.message

    def test__path_pattern_typo_raises_for_step_matrix_too(self, fx_read_csv):
        """step_matrix shares the same path_pattern validation as step_sankey
        (both delegate to StepMatrix.fit())."""
        df = fx_read_csv("tools/step_matrix_input.csv", sep="\t")
        stream = Eventstream(df)

        with pytest.raises(InvalidParameterError):
            stream.step_matrix_data(max_steps=2, path_pattern="typo_event")


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
