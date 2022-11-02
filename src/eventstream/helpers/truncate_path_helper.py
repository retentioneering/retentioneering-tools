from __future__ import annotations

from typing import Literal, Optional

from ..types import EventstreamType


class TruncatePathHelperMixin:
    def truncate_path(
        self,
        drop_before: Optional[str],
        drop_after: Optional[str],
        occurrence_before: Literal["first", "last"] = "first",
        occurrence_after: Literal["first", "last"] = "first",
        shift_before: int = 0,
        shift_after: int = 0,
    ) -> EventstreamType:
        # avoid circular import
        from src.data_processors_lib.rete import TruncatePath, TruncatePathParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore
        params = {
            "drop_before": drop_before,
            "drop_after": drop_after,
            "occurrence_before": occurrence_before,
            "occurrence_after": occurrence_after,
            "shift_before": shift_before,
            "shift_after": shift_after,
        }

        node = EventsNode(processor=TruncatePath(params=TruncatePathParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
