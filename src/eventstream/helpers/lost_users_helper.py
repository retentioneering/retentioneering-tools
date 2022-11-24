from __future__ import annotations

from typing import List, Optional, Tuple

from src.constants.constants import DATETIME_UNITS

from ..types import EventstreamType


class LostUsersHelperMixin:
    def lost_users(
        self, lost_cutoff: Optional[Tuple[float, DATETIME_UNITS]], lost_users_list: Optional[List[int]]
    ) -> EventstreamType:
        # avoid circular import
        from src.data_processors_lib.rete import LostUsersEvents, LostUsersParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=LostUsersEvents(
                params=LostUsersParams(lost_cutoff=lost_cutoff, lost_users_list=lost_users_list)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
