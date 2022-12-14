from __future__ import annotations

from ..types import EventstreamType


class StartEndHelperMixin:
    def add_start_end(self) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which creates two synthetic events in each user's path:
        ``path_start`` and ``path_end``. And adds them to the input ``eventstream``.

        Returns
        -------
        EventstreamType
            Input ``eventstream`` with new synthetic events.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:func:`src.data_processors_lib.rete.start_end_events.StartEndEvents`
        """
        # avoid circular import
        from src.data_processors_lib.rete import StartEndEvents, StartEndEventsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=StartEndEvents(params=StartEndEventsParams(**{})))
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
