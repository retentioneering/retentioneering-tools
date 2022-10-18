from __future__ import annotations

from abc import abstractmethod
from typing import Any, Literal, Protocol, TypedDict

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


class TransitionGraphProtocol(Protocol):
    graph_template: str = ""

    @abstractmethod
    def __init__(self, eventstream: EventstreamType, plot_params: PlotParams | None) -> None:
        ...

    @abstractmethod
    def calculate_graph(self, targets: dict[str, AllowedColors], thresh: float) -> None:
        ...

    @abstractmethod
    def show_graph(
        self,
        show_weights: bool = None,
        show_percents: bool = None,
        show_nodes_names: bool = None,
        show_all_edges_for_targets: bool = None,
        show_nodes_without_links: bool = None,
    ) -> Any:
        ...
