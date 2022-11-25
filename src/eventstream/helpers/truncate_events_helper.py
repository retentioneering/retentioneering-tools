from __future__ import annotations

from typing import Optional, Tuple

from src.constants import DATETIME_UNITS

from ..types import EventstreamType


class TruncatedEventsHelperMixin:
    def truncated_events(
        self,
        left_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
        right_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
    ) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import TruncatedEvents, TruncatedEventsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        params = dict(left_truncated_cutoff=left_truncated_cutoff, right_truncated_cutoff=right_truncated_cutoff)

        node = EventsNode(processor=TruncatedEvents(params=TruncatedEventsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
