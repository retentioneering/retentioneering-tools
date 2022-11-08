from __future__ import annotations

from typing import List, Literal, Union

from ..types import EventstreamType


class NewUsersHelperMixin:
    def add_new_users(self, new_users_list: Union[List[int], Literal["all"]]) -> EventstreamType:

        # avoid circular import
        from src.data_processors_lib.rete import NewUsersEvents, NewUsersParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=NewUsersEvents(params=NewUsersParams(new_users_list=new_users_list))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
