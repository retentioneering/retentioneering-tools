from __future__ import annotations

from typing import Literal, Union

from ..types import EventstreamType


class CollapseLoopsHelperMixin:
    def collapse_loops(
        self,
        suffix: Union[Literal["loop", "count"], None] = "loop",
        timestamp_aggregation_type: Literal["max", "min", "mean"] = "max",
    ) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import CollapseLoops, CollapseLoopsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=CollapseLoops(
                params=CollapseLoopsParams(
                    suffix=suffix, timestamp_aggregation_type=timestamp_aggregation_type  # type: ignore
                )
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
