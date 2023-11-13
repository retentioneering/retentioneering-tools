from __future__ import annotations

from typing import Callable, Optional

from pandas import DataFrame, Series

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamSchemaType, EventstreamType


class FilterEventsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(  # type: ignore
        scope="filter_events",
        event_name="helper",
        event_value="combine",
    )
    def filter_events(
        self: EventstreamType, func: Callable[[DataFrame, Optional[EventstreamSchemaType]], Series]
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that filters input ``eventstream`` based on custom conditions.

        Parameters
        ----------
            %(FilterEvents.parameters)s

        Returns
        -------
        Eventstream
            The filtered ``eventstream``.


        """

        calling_params = {
            "func": func,
        }

        # avoid circular import
        from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=FilterEvents(params=FilterEventsParams(func=func)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="filter_events",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
