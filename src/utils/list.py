# TODO fix me
from __future__ import annotations

from typing import Callable, List, Optional, TypeVar

T = TypeVar("T")


def find_item(l: List[T], cond: Callable[[T], bool]) -> Optional[T]:
    for item in l:
        if cond(item):
            return item
    return None


def find_index(l: List[T], cond: Callable[[T], bool]) -> int:
    for i, item in enumerate(l):
        if cond(item):
            return i
    return -1
