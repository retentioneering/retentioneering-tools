from __future__ import annotations

from typing import Callable, List, Optional

from ..types import EventstreamType


class AddPositiveEventsHelperMixin:
    def add_positive_events(self, targets: List[str], func: Optional[Callable] = None) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates new synthetic
        events in paths of all users having the specified events - ``positive_target_RAW_EVENT_NAME``.

        Parameters
        ----------
        See parameters description
            :py:class:`.AddPositiveEvents`

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.



        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            AddPositiveEvents,
            AddPositiveEventsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        params: dict[str, list[str] | Callable] = {"targets": targets}
        if func:
            params["func"] = func

        node = EventsNode(processor=AddPositiveEvents(params=AddPositiveEventsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
