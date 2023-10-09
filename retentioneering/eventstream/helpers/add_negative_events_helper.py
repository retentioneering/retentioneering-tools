from __future__ import annotations

from typing import Callable, List, Optional

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class AddNegativeEventsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="add_negative_events",
        event_name="helper",
        event_value="combine",
    )
    def add_negative_events(
        self: EventstreamType, targets: List[str], func: Optional[Callable] = None
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates new synthetic
        events in paths of all users having the specified events - ``negative_target_RAW_EVENT_NAME``.

        Parameters
        ----------
            %(AddNegativeEvents.parameters)s

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.



        """

        calling_params = {
            "targets": targets,
            "func": func,
        }

        # avoid circular import
        from retentioneering.data_processors_lib import (
            AddNegativeEvents,
            AddNegativeEventsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        params: dict[str, list[str] | Callable] = {"targets": targets}
        if func:
            params["func"] = func

        node = EventsNode(processor=AddNegativeEvents(params=AddNegativeEventsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="add_negative_events",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )
        return result
