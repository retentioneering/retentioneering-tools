from __future__ import annotations

from typing import Any, Callable

from pandas import DataFrame

from ..types import EventstreamSchemaType, EventstreamType


class FilterHelperMixin:
    def filter(self, func: Callable[[DataFrame, EventstreamSchemaType], Any]) -> EventstreamType:
        """
        A method of ``Eventstream`` class that filters input ``eventstream`` based on custom conditions.

        Returns
        -------
        Eventstream
            The filtered ``eventstream``.

        See Also
        --------
        :py:class:`.FilterEvents`
        """
        # avoid circular import
        from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=FilterEvents(params=FilterEventsParams(func=func)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
