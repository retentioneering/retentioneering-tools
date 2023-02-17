from __future__ import annotations

from typing import Literal

AGGREGATION_NAMES = Literal["mean", "median"]
EVENTSTREAM_GLOBAL_EVENTS = Literal["eventstream_start", "eventstream_end"]
BINS_ESTIMATORS = Literal["auto", "fd", "doane", "scott", "stone", "rice", "sturges", "sqrt"]
