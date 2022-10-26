from __future__ import annotations

from ..types import EventstreamType
from .typing import HelperProtocol


class StartEndHelperMixin(HelperProtocol):
    def add_start_end(self) -> EventstreamType:
        from src.data_processors_lib.rete import StartEndEvents, StartEndEventsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=StartEndEvents(params=StartEndEventsParams(**{})))
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        return result
