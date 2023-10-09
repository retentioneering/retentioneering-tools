from __future__ import annotations

from typing import List, Optional, Tuple

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.constants import DATETIME_UNITS
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class LabelLostUsersHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="label_lost_users",
        event_name="helper",
        event_value="combine",
    )
    def label_lost_users(
        self: EventstreamType,
        timeout: Optional[Tuple[float, DATETIME_UNITS]] = None,
        lost_users_list: Optional[List[int]] = None,
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates one
        of the synthetic events in each user's path: ``lost_user`` or ``absent_user`` .

        Parameters
        ----------
            %(LabelLostUsers.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events.



        """
        calling_params = {
            "timeout": timeout,
            "lost_users_list": lost_users_list,
        }
        not_hash_values = ["timeout"]

        # avoid circular import
        from retentioneering.data_processors_lib import (
            LabelLostUsers,
            LabelLostUsersParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=LabelLostUsers(
                params=LabelLostUsersParams(timeout=timeout, lost_users_list=lost_users_list)  # type: ignore
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="label_lost_users",
            event_name="metadata",
            called_params=calling_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
