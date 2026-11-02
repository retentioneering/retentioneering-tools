import pandas as pd
import pytest
from dataclasses import asdict

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.eventstream.ordering import is_laid_out_any_path_order


@pytest.fixture
def multi_path_stream():
    # z precedes a in source event order; their calendars overlap, including
    # generated inactive days. A second user checks partition isolation.
    return Eventstream(
        pd.DataFrame(
            [
                ["u1", "z", "buy", "2024-01-01 10:00", 10, "paid"],
                ["u1", "a", "buy", "2024-01-01 11:00", 20, "free"],
                ["u1", "z", "buy", "2024-01-03 10:00", 30, "paid"],
                ["u2", "m", "buy", "2024-01-04 10:00", 40, "other"],
            ],
            columns=["user_id", "session_id", "event", "timestamp", "revenue", "plan"],
        ),
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["plan"]},
    )


class TestDailyStatesPathGrain:
    def test_secondary_grain_preserves_ids_and_orders_overlapping_calendars(
        self, multi_path_stream
    ):
        source = multi_path_stream
        before = source.df.copy(deep=True)
        before_schema = asdict(source.schema)
        result = source.to_daily_states(path_col="session_id", max_dormant_days=2)
        df = result.df

        assert asdict(result.schema) == before_schema
        assert df["user_id"].tolist() == ["u1"] * 7 + ["u2"]
        assert df["session_id"].tolist() == ["z", "a", "z", "a", "z", "a", "z", "m"]
        assert df["timestamp"].dt.day.tolist() == [1, 1, 2, 2, 3, 3, 4, 4]
        assert df["event"].tolist() == [
            "new",
            "new",
            "at_risk_wau",
            "at_risk_wau",
            "current",
            "at_risk_wau",
            "at_risk_wau",
            "new",
        ]
        assert df["index"].tolist() == [1, 2, 3, 4, 5, 6, 7, 1]
        assert not df[result.schema.path_cols].isna().any().any()
        assert not df.duplicated(result.schema.order_cols).any()
        assert is_laid_out_any_path_order(df, result.schema)
        # Forward fill stays inside each session, including on synthetic days.
        assert df["revenue"].tolist() == [10, 20, 10, 20, 30, 20, 30, 40]
        assert df["plan"].tolist() == [
            "paid",
            "free",
            "paid",
            "free",
            "paid",
            "free",
            "paid",
            "other",
        ]
        pd.testing.assert_frame_equal(source.df, before)
        assert asdict(source.schema) == before_schema
        pd.testing.assert_frame_equal(
            df, source.to_daily_states(path_col="session_id", max_dormant_days=2).df
        )
        # Downstream readers and marker insertion use the same valid positions.
        assert result.get_metrics([{"metric": "length"}], path_col="session_id")[
            "length"
        ].to_dict() == {"a": 3, "m": 1, "z": 4}
        assert result.get_metrics([{"metric": "length"}], path_col="user_id")[
            "length"
        ].to_dict() == {"u1": 7, "u2": 1}
        result.add_events("marker", source_event="new")

    @pytest.mark.parametrize("path_col", [None, "user_id"])
    def test_primary_grain_removes_finer_ids(self, multi_path_stream, path_col):
        result = multi_path_stream.to_daily_states(
            path_col=path_col, max_dormant_days=0, agg={"revenue": "sum"}
        )
        assert result.schema.path_cols == ["user_id"]
        assert "session_id" not in result.df.columns
        assert "session_id" not in result.schema.custom_cols
        assert result.df["timestamp"].dt.day.tolist() == [1, 2, 3, 4]
        assert result.df["revenue"].tolist() == [30, 30, 30, 40]
        assert result.df["event"].tolist() == ["new", "at_risk_wau", "current", "new"]
        assert is_laid_out_any_path_order(result.df, result.schema)

    @pytest.mark.parametrize(
        "grain, expected",
        [
            ("account", ["account"]),
            ("user_id", ["account", "user_id"]),
            ("session_id", ["account", "user_id", "session_id"]),
        ],
    )
    def test_schema_is_a_prefix_of_declared_hierarchy(
        self, multi_path_stream, grain, expected
    ):
        df = multi_path_stream.df.copy()
        df["account"] = "company"
        df = df.drop(columns=["index", "subindex"])
        stream = Eventstream(df, {"path_cols": ["account", "user_id", "session_id"]})
        result = stream.to_daily_states(path_col=grain, max_dormant_days=0)
        assert result.schema.path_cols == expected
        assert set(result.df.columns) == set(result.schema.cols)
        assert not result.df[expected].isna().any().any()
        for finer in set(stream.schema.path_cols) - set(expected):
            assert finer not in result.df.columns
        assert is_laid_out_any_path_order(result.df, result.schema)

    @pytest.mark.parametrize("grain", ["user_id", "session_id"])
    @pytest.mark.parametrize("empty", [False, True])
    def test_schema_does_not_depend_on_observed_cardinality(
        self, multi_path_stream, grain, empty
    ):
        # A sample with a single session/user must have the same schema as an
        # empty sample or one with many sessions/user.
        stream = multi_path_stream.filter_events(
            func=lambda df: df["user_id"] == ("absent" if empty else "u2")
        )
        result = stream.to_daily_states(path_col=grain)
        expected = ["user_id"] if grain == "user_id" else ["user_id", "session_id"]
        assert result.schema.path_cols == expected
        assert set(result.df.columns) == set(result.schema.cols)
        assert result.df.empty == empty
        assert is_laid_out_any_path_order(result.df, result.schema)

    def test_first_last_and_same_day_ties_use_source_positions(self):
        stream = Eventstream(
            pd.DataFrame(
                {
                    "user_id": [1, 1, 1],
                    "session_id": [20, 10, 20],
                    "event": ["buy"] * 3,
                    "timestamp": [
                        "2024-01-01 12:00",
                        "2024-01-01 12:00",
                        "2024-01-01 08:00",
                    ],
                    "index": [5, 8, 10],
                    "revenue": [10, 20, 30],
                }
            ),
            {"path_cols": ["user_id", "session_id"]},
        )
        first = stream.to_daily_states(path_col="session_id", agg={"revenue": "first"})
        last = stream.to_daily_states(path_col="session_id", agg={"revenue": "last"})
        assert first.df["session_id"].tolist() == [20, 10]
        assert first.df["revenue"].tolist() == [10, 20]
        assert last.df["revenue"].tolist() == [30, 20]
        assert first.df["index"].tolist() == [1, 2]

    def test_activity_is_scoped_to_session_and_uses_local_calendar(self):
        stream = Eventstream(
            pd.DataFrame(
                {
                    "user_id": [1, 1, 1],
                    "session_id": ["s1", "s1", "s2"],
                    "event": ["buy", "buy", "view"],
                    "timestamp": pd.to_datetime(
                        ["2024-01-01 23:30", "2024-01-02 00:30", "2024-01-01 23:45"]
                    ).tz_localize("Europe/Belgrade"),
                }
            ),
            {"path_cols": ["user_id", "session_id"]},
        )
        result = stream.to_daily_states(path_col="session_id", active_events=["buy"])
        assert result.df["session_id"].tolist() == ["s1", "s2", "s1", "s2"]
        assert result.df["timestamp"].dt.day.tolist() == [1, 1, 2, 2]
        assert result.df["event"].tolist() == ["new", "dormant", "current", "dormant"]

    def test_custom_schema_names_are_quoted(self, multi_path_stream):
        names = {
            "user_id": 'user"id',
            "session_id": "session id",
            "event": "state",
            "timestamp": "date time",
            "index": "position",
            "subindex": "sub position",
        }
        stream = Eventstream(
            multi_path_stream.df.rename(columns=names),
            {
                "path_cols": [names["user_id"], names["session_id"]],
                "event_col": "state",
                "timestamp_col": "date time",
                "index": "position",
                "subindex": "sub position",
            },
        )
        result = stream.to_daily_states(path_col="session id")
        assert result.schema.path_cols == stream.schema.path_cols
        assert not result.df[result.schema.path_cols].isna().any().any()
        assert is_laid_out_any_path_order(result.df, result.schema)

    @pytest.mark.parametrize("grain", ["user_id", "session_id"])
    def test_empty_source_with_categorical_ids(self, grain):
        stream = Eventstream(
            pd.DataFrame(
                {
                    "user_id": pd.Categorical([], categories=["u1"]),
                    "session_id": pd.Categorical([], categories=["s1"]),
                    "event": pd.Series([], dtype="str"),
                    "timestamp": pd.Series([], dtype="datetime64[ns]"),
                }
            ),
            {"path_cols": ["user_id", "session_id"]},
        )
        result = stream.to_daily_states(path_col=grain)
        assert result.df.empty
        assert result.schema.path_cols == (
            ["user_id"] if grain == "user_id" else ["user_id", "session_id"]
        )
        assert set(result.df.columns) == set(result.schema.cols)
        assert result.df["subindex"].dtype == "float64"

    def test_internal_path_order_does_not_shadow_user_columns(self, multi_path_stream):
        df = multi_path_stream.df.copy()
        df["_daily_path_order"] = [100, 200, 300, 400]
        # DuckDB identifiers are case-insensitive.
        df["_DAILY_PATH_ORDER_"] = [1, 2, 3, 4]
        df["user_id"] = pd.Categorical(df["user_id"], categories=["u2", "u1"])
        df["session_id"] = pd.Categorical(df["session_id"], categories=["m", "a", "z"])
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        result = stream.to_daily_states(path_col="session_id", max_dormant_days=0)
        assert set(result.df.columns) == set(result.schema.cols)
        u1 = result.df.loc[result.df["user_id"] == "u1"]
        assert u1["session_id"].tolist() == ["z", "a", "z", "z"]
        assert u1["_daily_path_order"].tolist() == [100, 200, 100, 300]
        assert u1["_DAILY_PATH_ORDER_"].tolist() == [1, 2, 1, 3]
        assert is_laid_out_any_path_order(result.df, result.schema)

    @pytest.mark.parametrize("grain", ["user_id", "session_id"])
    @pytest.mark.parametrize("column", ["user_id", "session_id"])
    def test_agg_cannot_override_path_identity(self, multi_path_stream, grain, column):
        expected = multi_path_stream.to_daily_states(path_col=grain)
        actual = multi_path_stream.to_daily_states(path_col=grain, agg={column: "sum"})
        pd.testing.assert_frame_equal(actual.df, expected.df)
        assert actual.schema == expected.schema


class TestDailyStates:
    def test_first_row_is_new(self):
        df = pd.DataFrame(
            [
                ["u1", "login", "2023-01-01 10:00:00"],
                ["u1", "purchase", "2023-01-02 10:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.to_daily_states(max_dormant_days=30)
        states = res.df["event"].tolist()

        assert states[0] == "new"

    def test_active_events_filter(self):
        df = pd.DataFrame(
            [
                ["u1", "login", "2023-01-01 10:00:00"],
                ["u1", "purchase", "2023-01-02 10:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        # Only "purchase" counts as activity
        res = stream.to_daily_states(active_events=["purchase"], max_dormant_days=5)
        first_state = res.df.loc[res.df["event"] != "path_start", "event"].iloc[0]

        # Day 1 had only "login" → not active → dormant or at_risk_*
        assert first_state in {"dormant", "at_risk_wau", "at_risk_mau"}

    def test_subsequent_active_day_is_current(self):
        df = pd.DataFrame(
            [
                ["u1", "login", "2023-01-01 10:00:00"],
                ["u1", "login", "2023-01-03 10:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.to_daily_states(max_dormant_days=5)
        states = res.df["event"].tolist()

        # Jan-01 → new, Jan-03 → current (was active within last 7 days)
        assert "new" in states
        assert "current" in states


class TestDailyStatesAgg:
    @staticmethod
    def _revenue_stream() -> Eventstream:
        df = pd.DataFrame(
            [
                ["u1", "buy", "2023-01-01 10:00:00", 10],
                ["u1", "buy", "2023-01-01 11:00:00", 5],
                ["u1", "buy", "2023-01-02 10:00:00", 100],
                ["u1", "buy", "2023-01-02 12:00:00", 1],
            ],
            columns=["user_id", "event", "timestamp", "revenue"],
        )
        return Eventstream(df, {"custom_cols": ["revenue"]})

    def test_agg_sum_adds_the_days_values(self):
        """`sum` adds up each day's values: `[10, 5]` and `[100, 1]`."""
        res = self._revenue_stream().to_daily_states(
            agg={"revenue": "sum"}, max_dormant_days=0
        )
        assert res.df["revenue"].tolist() == [15, 101]

    def test_unknown_agg_raises_and_names_this_processor(self):
        with pytest.raises(PreprocessingConfigError) as exc:
            self._revenue_stream().to_daily_states(agg={"revenue": "nope"})
        assert "to_daily_states" in str(exc.value)
