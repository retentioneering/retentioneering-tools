from __future__ import annotations

from ..types import EventstreamType


class AddStartEndEventsHelperMixin:
    def add_start_end_events(self) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates
        two synthetic events in each user's path: ``path_start`` and ``path_end``.

        Returns
        -------
        Eventstream
            Input ``eventstream`` with added synthetic events. See details :py:class:`.AddStartEndEvents`.


        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            AddStartEndEvents,
            AddStartEndEventsParams,
        )
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=AddStartEndEvents(params=AddStartEndEventsParams(**{})))
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
