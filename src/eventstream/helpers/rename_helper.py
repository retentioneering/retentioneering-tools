from __future__ import annotations

from ..types import EventstreamType


class RenameHelperMixin:
    def rename(self, rules: list[dict[str, list[str]]]) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib import RenameParams, RenameProcessor
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=RenameProcessor(params=RenameParams(rules=rules)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
