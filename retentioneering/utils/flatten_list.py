from __future__ import annotations

from typing import Collection


def flatten(l: Collection) -> list:
    result = []
    for item in l:
        if isinstance(item, Collection) and not isinstance(item, str):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result
