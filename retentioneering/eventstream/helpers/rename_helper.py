from __future__ import annotations

from ..types import EventstreamType


class RenameHelperMixin:
    def rename(self, rules: list[dict[str, list[str] | str]]) -> EventstreamType:
        # avoid circular import
        from retentioneering.data_processors_lib import RenameParams, RenameProcessor
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.preprocessing_graph import PreprocessingGraph

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=RenameProcessor(params=RenameParams(rules=rules)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
