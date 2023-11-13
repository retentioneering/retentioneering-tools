from __future__ import annotations

from typing import Any, Callable, Optional

import pandas as pd

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamSchemaType, EventstreamType

EventstreamFilter = Callable[[pd.DataFrame, Optional[EventstreamSchemaType]], Any]


class GroupEventsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(  # type: ignore
        scope="group_events",
        event_name="helper",
        event_value="combine",
    )
    def group_events(
        self: EventstreamType,
        event_name: str,
        func: EventstreamFilter,
        event_type: str | None = "group_alias",
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that filters and replaces raw events with new synthetic events,
        having the same ``timestamp`` and ``user_id``, but new ``event_name``.

        Parameters
        ----------
            %(GroupEvents.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with replaced events.



        """

        calling_params = {
            "event_name": event_name,
            "func": func,
            "event_type": event_type,
        }

        # avoid circular import
        from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=GroupEvents(
                params=GroupEventsParams(event_name=event_name, func=func, event_type=event_type)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="group_events",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
