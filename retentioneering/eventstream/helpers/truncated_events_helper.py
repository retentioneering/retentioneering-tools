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
        A method of ``Eventstream`` class that creates new synthetic event(s) for each user based
        on the timeout threshold: ``truncated_left`` and ``truncated_right``.


        Parameters
        ----------
        See parameters description
            :py:class:`.TruncatedEvents`

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.


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
