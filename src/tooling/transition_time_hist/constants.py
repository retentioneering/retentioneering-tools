from __future__ import annotations

from typing import Literal

HIST_TYPE_NAMES = Literal["adjacent", "event_pair"]
AGGREGATION_NAMES = Literal["user_mean", "user_median"]
TIMEDELTA_UNIT_NAMES = Literal["seconds", "minutes", "hours", "days"]
