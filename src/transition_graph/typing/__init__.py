from __future__ import annotations

from abc import abstractmethod
from typing import Any, List, Literal, Protocol, TypedDict

from eventstream.types import EventstreamType


class PlotParams(TypedDict):
    pass


class TransitionGraphPlotProtocol(Protocol):
    @abstractmethod
    def __init__(self, plot_params: PlotParams) -> None:
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
        graph: dict,  # preprocessed graph
        plot_params: PlotParams | None,
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
