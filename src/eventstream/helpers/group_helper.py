from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from ..types import EventstreamSchemaType, EventstreamType

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchemaType], Any]


class GroupHelperMixin:
    def group(
        self,
        event_name: str,
        filter: EventstreamFilter,
        event_type: str | None = "group_alias",
    ) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import GroupEvents, GroupEventsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=GroupEvents(
                params=GroupEventsParams(event_name=event_name, filter=filter, event_type=event_type)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result