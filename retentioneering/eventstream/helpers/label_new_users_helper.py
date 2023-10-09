from __future__ import annotations

from typing import List, Literal, Union

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class LabelNewUsersHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="label_new_users",
        event_name="helper",
        event_value="combine",
    )
    def label_new_users(self: EventstreamType, new_users_list: Union[List[int], Literal["all"]]) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates one
        of the synthetic events in each user's path: ``new_user`` or ``existing_user`` .

        Parameters
        ----------
            %(LabelNewUsers.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.


        """
        calling_params = {"new_users_list": new_users_list}

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
        collect_data_performance(
            scope="label_new_users",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
