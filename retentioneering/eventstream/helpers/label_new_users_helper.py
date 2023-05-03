from __future__ import annotations

from typing import List, Literal, Union

from retentioneering.backend.tracker import track

from ..types import EventstreamType


class LabelNewUsersHelperMixin:
    @track(  # type: ignore
        tracking_info={"event_name": "helper"},
        scope="label_new_users",
        event_value="combine",
        allowed_params=[
            "new_users_list",
        ],
    )
    def label_new_users(self, new_users_list: Union[List[int], Literal["all"]]) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates one
        of the synthetic events in each user's path: ``new_user`` or ``existing_user`` .

        Parameters
        ----------
        See parameters description
            :py:class:`.LabelNewUsers`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.


        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            LabelNewUsers,
            LabelNewUsersParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=LabelNewUsers(params=LabelNewUsersParams(new_users_list=new_users_list))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
