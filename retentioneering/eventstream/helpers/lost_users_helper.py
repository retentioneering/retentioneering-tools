from __future__ import annotations

from typing import List, Optional, Tuple

from retentioneering.constants import DATETIME_UNITS

from ..types import EventstreamType


class LostUsersHelperMixin:
    def lost_users(
        self, lost_cutoff: Optional[Tuple[float, DATETIME_UNITS]] = None, lost_users_list: Optional[List[int]] = None
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which creates one of synthetic events in each user's path:
        ``lost_user`` or ``absent_user``. And adds them to the input ``eventstream``.

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:class:`.LostUsersEvents`

        """

        # avoid circular import
        from retentioneering.data_processors_lib import LostUsersEvents, LostUsersParams
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=LostUsersEvents(
                params=LostUsersParams(lost_cutoff=lost_cutoff, lost_users_list=lost_users_list)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
