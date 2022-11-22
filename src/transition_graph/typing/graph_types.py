from __future__ import annotations

from abc import abstractmethod
from typing import (
    Any,
    List,
    Literal,
    MutableMapping,
    Protocol,
    Sequence,
    TypedDict,
    Union,
)

from eventstream.types import EventstreamType


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


class TransitionGraphPlotProtocol(Protocol):
    @abstractmethod
    def __init__(self, plot_params: PlotParamsType) -> None:
        ...

    @abstractmethod
    def render(self) -> Any:
        ...


AllowedColors = Literal["red", "green", "yellow", "blue", "magenta", "cyan"]


class Node:
    pass


class Edge:
    pass


class TransitionGraphProtocol(Protocol):
    graph_template: str = ""

    @abstractmethod
    def __init__(
        self,
        eventstream: EventstreamType,
        # graph: dict,  # preprocessed graph
        plot_params: PlotParamsType | None,
        nodes: List[Node],
        edges: List[Edge],
    ) -> None:
        ...

    @abstractmethod
    def calculate_graph(
        self,
        targets: dict[str, AllowedColors],
    ) -> None:
        ...

    @abstractmethod
    def show_graph(self) -> Any:
        ...


Threshold = MutableMapping[str, float]
NodeParams = MutableMapping[str, str]
Position = MutableMapping[str, Sequence[float]]


class GraphSettings(PlotParamsType):
    show_weights: bool
    show_percents: bool
    show_nodes_names: bool
    show_all_edges_for_targets: bool
    show_nodes_without_links: bool
    nodes_threshold: Threshold
    links_threshold: Threshold


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
