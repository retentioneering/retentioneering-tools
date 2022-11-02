from __future__ import annotations

from typing import Callable, List

from ..types import EventstreamType


class NegativeTargetHelperMixin:
    def negative_target(self, negative_target_events: List[str], negative_function: Callable) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import NegativeTarget, NegativeTargetParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=NegativeTarget(
                params=NegativeTargetParams(
                    negative_target_events=negative_target_events, negative_function=negative_function  # type: ignore
                )
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
