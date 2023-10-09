from __future__ import annotations

from ..types import EventstreamType


class RenameHelperMixin:
    def rename(self: EventstreamType, rules: list[dict[str, list[str] | str]]) -> EventstreamType:
        # avoid circular import
        from retentioneering.data_processors_lib import RenameParams, RenameProcessor
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=RenameProcessor(params=RenameParams(rules=rules)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
