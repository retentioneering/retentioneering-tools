from __future__ import annotations

from typing import Callable

from pandas import DataFrame, Series

from retentioneering.backend.tracker import track

from ..types import EventstreamSchemaType, EventstreamType


class FilterEventsHelperMixin:
    @track(  # type: ignore
        tracking_info={"event_name": "helper"},
        scope="add_negative_events",
        event_value="combine",
        allowed_params=[
            "func",
        ],
    )
    def filter_events(self, func: Callable[[DataFrame, EventstreamSchemaType], Series]) -> EventstreamType:
        """
        A method of ``Eventstream`` class that filters input ``eventstream`` based on custom conditions.

        Parameters
        ----------
        See parameters description
            :py:class:`.FilterEvents`

        Returns
        -------
        Eventstream
            The filtered ``eventstream``.


        """
        # avoid circular import
        from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=FilterEvents(params=FilterEventsParams(func=func)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
