from __future__ import annotations

from typing import List, Literal, Union

from ..types import EventstreamType


class NewUsersHelperMixin:
    def add_new_users(self, new_users_list: Union[List[int], Literal["all"]]) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates one
        of the synthetic events in each user's path: ``new_user`` or ``existing_user`` .

        Parameters
        ----------
        See parameters description
            :py:class:`.NewUsersEvents`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.


        """
        # avoid circular import
        from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=NewUsersEvents(params=NewUsersParams(new_users_list=new_users_list))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
