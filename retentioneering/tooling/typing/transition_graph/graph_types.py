from __future__ import annotations

from typing import (
    Any,
    Dict,
    List,
    Literal,
    MutableMapping,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

from typing_extensions import NotRequired

from retentioneering.tooling.transition_graph.interface import TargetId

RenameRule = Dict[str, Union[List[str], str]]


class Degree(TypedDict):
    degree: float
    source: float


class PreparedNode(TypedDict):
    index: int
    name: str
    degree: MutableMapping[str, Degree] | dict[str, Any]
    changed_name: str | None
    type: str
    x: NotRequired[float]
    y: NotRequired[float]
    active: bool
    alias: str
    parent: str


class PlotParamsType(TypedDict):
    pass


AllowedColors = Literal["red", "green", "yellow", "blue", "magenta", "cyan"]

# @FIXME: idk why import annotation not fixed this cause, and I need to use Union. Vladimir Makhanov
Threshold = Dict[str, Union[float, int]]  # type: ignore
NodeParams = MutableMapping[str, Optional[Union[str, TargetId]]]
Position = MutableMapping[str, Sequence[float]]


class GraphSettings(PlotParamsType):
    show_weights: bool
    show_percents: bool
    show_nodes_names: bool
    show_all_edges_for_targets: bool
    show_nodes_without_links: bool


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


NormType = Union[Literal["full", "node", "none"], None]
