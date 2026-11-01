"""Timezone of a pandas timestamp column, as a name DuckDB understands."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from retentioneering.exceptions import SchemaConfigError


def column_timezone(series: pd.Series) -> str | None:
    """
    The IANA name or fixed offset of a tz-aware timestamp column, or None if
    the column is tz-naive.

    Covers `zoneinfo` and `pytz` zones (by name) and `datetime.timezone` fixed
    offsets (`"UTC"`, `"UTC+03:00"`). Other tz objects, such as `dateutil`
    zones, have no name DuckDB can read, so they raise rather than fall back to
    a zone that would put events on the wrong day.
    """
    if not isinstance(series.dtype, pd.DatetimeTZDtype):
        return None
    tz = series.dtype.tz
    name = getattr(tz, "key", None) or getattr(tz, "zone", None)
    if name:
        return str(name)
    if isinstance(tz, dt.timezone):
        return str(tz)
    raise SchemaConfigError(
        f"timestamp column {series.name!r} has timezone {tz!r}, which cannot be "
        "used for calendar days. Convert it to a named zone first, e.g. "
        "`df[col].dt.tz_convert('Europe/Moscow')`."
    )
