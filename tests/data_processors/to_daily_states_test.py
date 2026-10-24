import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


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
        """The `agg` docstring's own example — `{"revenue": "sum"}`."""
        res = self._revenue_stream().to_daily_states(
            agg={"revenue": "sum"}, max_dormant_days=0
        )
        assert res.df["revenue"].tolist() == [15, 101]

    def test_unknown_agg_raises_and_names_this_processor(self):
        with pytest.raises(PreprocessingConfigError) as exc:
            self._revenue_stream().to_daily_states(agg={"revenue": "nope"})
        assert "to_daily_states" in str(exc.value)
