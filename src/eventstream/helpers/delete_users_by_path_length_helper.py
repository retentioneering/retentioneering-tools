from __future__ import annotations

from typing import Tuple

from src.data_processors_lib.rete.constants import DATETIME_UNITS

from ..types import EventstreamType


class DeleteUsersByPathLengthHelperMixin:
    def delete_users(
        self, events_num: int | None = None, cutoff: Tuple[float, DATETIME_UNITS] | None = None
    ) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import (
            DeleteUsersByPathLength,
            DeleteUsersByPathLengthParams,
        )
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=DeleteUsersByPathLength(
                params=DeleteUsersByPathLengthParams(events_num=events_num, cutoff=cutoff)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
