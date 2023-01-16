from __future__ import annotations

from typing import Optional, Tuple

from retentioneering.constants import DATETIME_UNITS

from ..types import EventstreamType


class TruncatedEventsHelperMixin:
    def truncated_events(
        self,
        left_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
        right_truncated_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which creates new synthetic event(s) for each user on the
        base of timeout threshold: ``truncated_left`` and ``truncated_right``.
        And adds them to the input ``eventstream``.

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:func:`src.data_processors_lib.truncated_events.TruncatedEvents`
        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            TruncatedEvents,
            TruncatedEventsParams,
        )
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        params = dict(left_truncated_cutoff=left_truncated_cutoff, right_truncated_cutoff=right_truncated_cutoff)

        node = EventsNode(processor=TruncatedEvents(params=TruncatedEventsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
