from __future__ import annotations

from typing import List, Optional, Tuple

from retentioneering.constants import DATETIME_UNITS

from ..types import EventstreamType


class LostUsersHelperMixin:
    def lost_users(
        self, timeout: Optional[Tuple[float, DATETIME_UNITS]] = None, lost_users_list: Optional[List[int]] = None
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates one
        of the synthetic events in each user's path: ``lost_user`` or ``absent_user`` .

        Parameters
        ----------
        See parameters description
            :py:class:`.LostUsersEvents`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.



        """

        # avoid circular import
        from retentioneering.data_processors_lib import LostUsersEvents, LostUsersParams
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=LostUsersEvents(
                params=LostUsersParams(timeout=timeout, lost_users_list=lost_users_list)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
