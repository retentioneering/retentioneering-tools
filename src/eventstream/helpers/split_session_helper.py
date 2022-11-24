from __future__ import annotations

from typing import Optional, Tuple

from src.constants.constants import DATETIME_UNITS

from ..types import EventstreamType


class SplitSessionsHelperMixin:
    def split_sessions(
        self,
        session_cutoff: Tuple[float, DATETIME_UNITS],
        session_col: str,
        mark_truncated: Optional[bool] = False,
    ) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import SplitSessions, SplitSessionsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore
        params = dict(session_cutoff=session_cutoff, session_col=session_col, mark_truncated=mark_truncated)
        node = EventsNode(processor=SplitSessions(params=SplitSessionsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
