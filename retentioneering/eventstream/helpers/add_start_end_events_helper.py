from __future__ import annotations

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class AddStartEndEventsHelperMixin:
    @docstrings.dedent
    @time_performance(
        scope="add_start_end_events",
        event_name="helper",
        event_value="combine",
    )
    def add_start_end_events(self: EventstreamType) -> EventstreamType:
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
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=AddStartEndEvents(params=AddStartEndEventsParams(**{})))
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="add_start_end_events",
            event_name="metadata",
            called_params={},
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
