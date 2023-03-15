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


class PlotParamsType(TypedDict):
    pass


AllowedColors = Literal["red", "green", "yellow", "blue", "magenta", "cyan"]

Threshold = MutableMapping[str, float | int]  # type: ignore
NodeParams = MutableMapping[str, Optional[str]]
Position = MutableMapping[str, Sequence[float]]


class GraphSettings(PlotParamsType):
    show_weights: bool
    show_percents: bool
    show_nodes_names: bool
    show_all_edges_for_targets: bool
    show_nodes_without_links: bool
    nodes_threshold: Threshold  # type: ignore
    links_threshold: Threshold  # type: ignore


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
