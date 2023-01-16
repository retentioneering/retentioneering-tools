from __future__ import annotations

from typing import Literal

# https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units
DATETIME_UNITS = Literal["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"]
