from __future__ import annotations

from typing import Any, Literal, MutableMapping, Optional, Sequence, TypedDict, Union


class Degree(TypedDict):
    degree: float
    source: float


class PreparedNode(TypedDict):
    index: int
    name: str
    degree: MutableMapping[str, Degree] | dict[str, Any]
    changed_name: str | None
    type: str
    x: float | None
    y: float | None
    active: bool
    alias: str
    parent: str


AllowedColors = Literal["red", "green", "yellow", "blue", "magenta", "cyan"]

# @FIXME: idk why import annotation not fixed this cause, and I need to use Union. Vladimir Makhanov
Threshold = MutableMapping[str, Union[float, int]]  # type: ignore
NodeParams = MutableMapping[str, Optional[str]]
Position = MutableMapping[str, Sequence[float]]


class Weight(TypedDict):
    weight_norm: float
    weight: float


class PreparedLink(TypedDict):
    sourceIndex: int
    targetIndex: int
    weights: MutableMapping[str, Weight]
    type: str


class LayoutNode(TypedDict):
    name: str
    x: float
    y: float


NormType = Union[Literal["full", "node"], None]
