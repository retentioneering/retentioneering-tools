"""Calendar days are taken in the timestamp column's own timezone.

DuckDB resolves a `TIMESTAMP WITH TIME ZONE` to a calendar day in its session
timezone, which defaults to the host's. These tests pin that setting to a zone
different from the data's for the duration of the test, so the assertions hold
on any machine — including a UTC CI runner, where the defect is invisible.

`SET GLOBAL` is what the fixture uses because the per-call cursors
`engine.run` executes on do not inherit a connection-level `SET`.
"""

import datetime as dt

import pandas as pd
import pytest

from retentioneering import engine
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import SchemaConfigError

# 23:30 and 00:30 local time: two calendar days in the column's own zone, but a
# single one in any zone far enough away, which makes the wrong zone observable.
LOCAL_TIMESTAMPS = ["2024-01-01 23:30:00", "2024-01-02 00:30:00"]
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


def _stream(tz, values=("a", "a")) -> Eventstream:
    timestamps = pd.to_datetime(LOCAL_TIMESTAMPS)
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


def _active_days(tz) -> list:
    return _stream(tz).get_metrics([{"metric": "active_days"}])["active_days"].tolist()


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

    @pytest.mark.parametrize(
        "tz",
        [
            "Europe/Moscow",
            # Behind UTC: both events fall on Jan 2 in UTC, and on one day too
            # if the offset's sign were read the POSIX way round.
            dt.timezone(dt.timedelta(hours=-5)),
        ],
        ids=["named", "fixed-offset"],
    )
    def test_day_is_taken_in_the_columns_zone(self, eastern_session_timezone, tz):
        assert _active_days(tz) == [2]

    def test_to_daily_states_uses_the_columns_zone(
        self, eastern_session_timezone
    ) -> None:
        assert len(_stream("Europe/Moscow").to_daily_states().df) == 2

    def test_a_zone_without_a_name_raises(self) -> None:
        dateutil_tz = pytest.importorskip("dateutil.tz")
        with pytest.raises(SchemaConfigError, match="tz_convert"):
            _active_days(dateutil_tz.gettz("Europe/Moscow"))

    def test_tz_naive_timestamps_are_unaffected(self, eastern_session_timezone) -> None:
        # A tz-naive column carries no zone, so its days are read off the
        # literal values and the session timezone must not enter at all.
        assert _active_days(None) == [2]

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
