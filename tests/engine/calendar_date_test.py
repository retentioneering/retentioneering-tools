"""Calendar-day bucketing must not depend on the host's timezone.

DuckDB resolves a `TIMESTAMP WITH TIME ZONE` to a calendar day in its session
timezone, which defaults to the host's. These tests pin that setting to a
non-UTC zone for the duration of the test so the assertions hold on any
machine — including a UTC CI runner, where the defect is invisible.

`SET GLOBAL` is what the fixture uses because the per-call cursors
`engine.run` executes on do not inherit a connection-level `SET`.
"""

import pandas as pd
import pytest

from retentioneering import engine
from retentioneering.eventstream.eventstream import Eventstream

# 23:30 and 00:30 UTC: two distinct UTC calendar days, but a single one in any
# zone far enough east, which is what makes the host's zone observable.
UTC_TIMESTAMPS = ["2024-01-01 23:30:00", "2024-01-02 00:30:00"]
AWAY_ZONE = "Asia/Tokyo"


@pytest.fixture
def eastern_session_timezone():
    """Run the body as if the host were in `AWAY_ZONE`."""
    conn = engine._root()
    original = conn.cursor().execute("SELECT current_setting('TimeZone')").fetchone()[0]
    conn.execute(f"SET GLOBAL TimeZone = '{AWAY_ZONE}'")
    try:
        yield
    finally:
        conn.execute(f"SET GLOBAL TimeZone = '{original}'")


def _stream(tz: str | None, values=("a", "a")) -> Eventstream:
    timestamps = pd.to_datetime(UTC_TIMESTAMPS)
    if tz is not None:
        timestamps = timestamps.tz_localize(tz)
    df = pd.DataFrame(
        {
            "user_id": ["user_1"] * len(values),
            "event": list(values),
            "timestamp": timestamps,
        }
    )
    return Eventstream(df, {"event_col": "event"})


class TestCalendarDayIsTimezoneIndependent:
    def test_active_days_metric(self, eastern_session_timezone) -> None:
        result = _stream("UTC").get_metrics([{"metric": "active_days"}])
        assert result["active_days"].tolist() == [2]

    def test_to_daily_states_emits_one_row_per_utc_day(
        self, eastern_session_timezone
    ) -> None:
        result = _stream("UTC").to_daily_states().df
        assert len(result) == 2

    def test_collapse_events_names_a_group_by_active_days(
        self, eastern_session_timezone
    ) -> None:
        # `active_days` is one of the metrics a `name` case condition may use,
        # so the same day bucketing decides what the merged event is called.
        result = _stream("UTC").collapse_events(
            loops=True,
            name=[
                {
                    "condition": {
                        "metric": "active_days",
                        "op": ">=",
                        "value": 2,
                    },
                    "name": "spanned_two_days",
                },
                "single_day",
            ],
        )
        assert result.df["event"].tolist() == ["spanned_two_days"]

    def test_tz_naive_timestamps_are_unaffected(self, eastern_session_timezone) -> None:
        # A tz-naive column carries no zone, so its days are read off the
        # literal values and the session timezone must not enter at all.
        assert _stream(None).get_metrics([{"metric": "active_days"}])[
            "active_days"
        ].tolist() == [2]

    def test_tz_naive_same_day_stays_one_day(self, eastern_session_timezone) -> None:
        df = pd.DataFrame(
            {
                "user_id": ["user_1", "user_1"],
                "event": ["a", "a"],
                "timestamp": pd.to_datetime(
                    ["2024-01-01 10:00:00", "2024-01-01 20:00:00"]
                ),
            }
        )
        result = Eventstream(df, {"event_col": "event"}).get_metrics(
            [{"metric": "active_days"}]
        )
        assert result["active_days"].tolist() == [1]
