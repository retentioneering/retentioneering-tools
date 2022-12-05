from __future__ import annotations

from ..types import EventstreamType


class MergeHelperMixin:
    def merge(self, rules: list[dict[str, str]]) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import MergeParams, MergeProcessor
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=MergeProcessor(params=MergeParams(rules=rules)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
