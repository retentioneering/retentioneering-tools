from __future__ import annotations

from collections import defaultdict

UOM_DICT: dict[str | None, int] = defaultdict(lambda: 1)
UOM_DICT["s"] = 1
UOM_DICT["m"] = 60
UOM_DICT["h"] = 3600
UOM_DICT["d"] = 24 * 3600
