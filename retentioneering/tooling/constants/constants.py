from __future__ import annotations

from typing import Literal

# https://numpy.org/doc/stable/reference/generated/numpy.histogram_bin_edges.html
BINS_ESTIMATORS = Literal["auto", "fd", "doane", "scott", "stone", "rice", "sturges", "sqrt"]
