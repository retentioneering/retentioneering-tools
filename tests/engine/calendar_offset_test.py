"""Calendar dates agree with pandas across offsets, DST and session zones."""

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.utils.timezones import column_timezone


FIXED_ZONES = [
    pytest.param(dt.timezone.utc, id="utc"),
    pytest.param(dt.timezone(dt.timedelta(hours=-5)), id="minus-5"),
    pytest.param(dt.timezone(dt.timedelta(hours=3)), id="plus-3"),
    pytest.param(dt.timezone(dt.timedelta(hours=-3, minutes=-30)), id="minus-3-30"),
    pytest.param(dt.timezone(dt.timedelta(hours=5, minutes=45)), id="plus-5-45"),
    pytest.param(
        dt.timezone(dt.timedelta(hours=5, minutes=30, seconds=45)),
        id="offset-with-seconds",
    ),
    # The display name must not override the actual offset.
    pytest.param(dt.timezone(dt.timedelta(hours=-5), "UTC"), id="custom-name"),
]


@pytest.fixture(params=["UTC", "Asia/Tokyo", "America/New_York"])
def session_timezone(request):
    conn = engine._root()
    with conn.cursor() as cur:
        original = cur.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    conn.execute(f"SET GLOBAL TimeZone = '{request.param}'")
    try:
        yield
    finally:
        conn.execute(f"SET GLOBAL TimeZone = '{original}'")


@pytest.mark.parametrize(
    "tz",
    FIXED_ZONES
    + [
        pytest.param(None, id="naive"),
        pytest.param("America/New_York", id="named-dst"),
        pytest.param(ZoneInfo("Europe/Berlin"), id="zoneinfo-dst"),
        pytest.param("Asia/Kathmandu", id="named-fractional"),
    ],
)
def test_calendar_dates_match_pandas(tz, session_timezone):
    # Exact local midnight on either side, in winter/summer and on both US
    # and European DST transition dates. Includes leap day and year rollover.
    days = pd.to_datetime(
        [
            "2024-01-01",
            "2024-02-29",
            "2024-03-10",
            "2024-03-31",
            "2024-07-01",
            "2024-10-27",
            "2024-11-03",
            "2025-01-01",
        ]
    )
    values = pd.DatetimeIndex(
        [
            day + delta
            for day in days
            for delta in [
                pd.Timedelta(seconds=-1),
                pd.Timedelta(0),
                pd.Timedelta(seconds=1),
            ]
        ]
    )
    if tz is not None:
        values = values.tz_localize(tz)
    frame = pd.DataFrame({"timestamp": values, "position": range(len(values))})
    day = dialect.calendar_date('"timestamp"', column_timezone(frame["timestamp"]))
    result = engine.run(
        f"SELECT {day} AS day FROM frame ORDER BY position", frame=frame
    )
    assert result["day"].dt.date.tolist() == frame["timestamp"].dt.date.tolist()


@pytest.mark.parametrize("tz", FIXED_ZONES)
def test_fixed_offsets_reach_all_calendar_consumers(tz, session_timezone):
    frame = pd.DataFrame(
        {
            "user_id": ["u", "u"],
            "event": ["a", "a"],
            "timestamp": pd.to_datetime(
                ["2024-01-01 23:30:00", "2024-01-02 00:30:00"]
            ).tz_localize(tz),
        }
    )
    stream = Eventstream(frame)
    assert stream.get_metrics([{"metric": "active_days"}])["active_days"].tolist() == [
        2
    ]
    daily = stream.to_daily_states().df
    assert daily["timestamp"].dt.date.tolist() == [
        dt.date(2024, 1, 1),
        dt.date(2024, 1, 2),
    ]
    collapsed = stream.collapse_events(
        loops=True,
        name=[
            {
                "condition": {"metric": "active_days", "op": ">=", "value": 2},
                "name": "two_days",
            },
            "one_day",
        ],
    )
    assert collapsed.df["event"].tolist() == ["two_days"]
