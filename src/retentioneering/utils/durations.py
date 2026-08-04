"""
Duration parsing for public-API inputs (ADR-0008).

One concept, one parser: every user-facing duration in the library — session
timeouts, collapse windows, anchor offsets — is either a pandas-parseable
duration string with an explicit unit (``"30m"``) or a ``pandas.Timedelta``,
and all of them come through here.
"""

from __future__ import annotations

import pandas as pd

__all__ = ["parse_duration"]


def parse_duration(value, *, param: str = "duration") -> float:
    """
    Convert a public-API duration value into seconds.

    Accepts a pandas-parseable duration string with an explicit unit
    (e.g. "30m", "1h", "1800s") or a pandas.Timedelta. Bare numbers are
    rejected to avoid unit ambiguity.

    `param` names the caller's user-facing parameter so the error message
    points at ``timeout`` / ``offset`` rather than at this helper.
    """
    if isinstance(value, pd.Timedelta):
        return float(value.total_seconds())
    if isinstance(value, str):
        # pd.Timedelta("1800") silently means 1800 *nanoseconds* — reject
        # unit-less strings outright instead of inheriting that footgun.
        if not any(c.isalpha() for c in value):
            raise ValueError(
                f"{param} {value!r} has no unit. "
                "Use a duration string with an explicit unit, e.g. '30m', '1h', '1800s'."
            )
        try:
            return float(pd.Timedelta(value).total_seconds())
        except ValueError as exc:
            raise ValueError(
                f"invalid {param} {value!r}: {exc}. "
                "Use a duration string with an explicit unit, e.g. '30m', '1h', '1800s'."
            ) from exc
    raise ValueError(
        f"{param} must be a duration string with an explicit unit "
        f"(e.g. '30m', '1h', '1800s') or a pandas.Timedelta, got {type(value).__name__}"
    )
