from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from retentioneering.backend.tracker import track

from ..types import EventstreamSchemaType, EventstreamType

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchemaType], Any]


class GroupEventsHelperMixin:
    @track(  # type: ignore
        tracking_info={"event_name": "helper"},
        scope="group_events",
        event_value="combine",
        allowed_params=[
            "event_name",
            "func",
            "event_type",
        ],
    )
    def group_events(
        self,
        event_name: str,
        func: EventstreamFilter,
        event_type: str | None = "group_alias",
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that filters and replaces raw events with new synthetic events,
        having the same ``timestamp`` and ``user_id``, but new ``event_name``.

        Parameters
        ----------
        See parameters description
            :py:class:`.GroupEvents`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with replaced events.



        """

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
        return result
