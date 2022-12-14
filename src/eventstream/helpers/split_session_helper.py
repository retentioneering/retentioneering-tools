from __future__ import annotations

from typing import Optional, Tuple

from src.constants import DATETIME_UNITS

from ..types import EventstreamType


class SplitSessionsHelperMixin:
    def split_sessions(
        self,
        session_cutoff: Tuple[float, DATETIME_UNITS],
        session_col: str = "session_id",
        mark_truncated: Optional[bool] = False,
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which creates new synthetic events in each user's path:
        ``session_start`` (or ``session_start_truncated``) and ``session_end`` (or ``session_end_truncated``).
        Those events divide user's paths on sessions.
        Also creates new column which contains session number for each event in input eventstream
        Session number will take the form: ``{user_id}_{session_number through one user path}``
        And adds those events and the new column to the input ``eventstream``.

        Returns
        -------
        EventstreamType
             Input ``eventstream`` with new synthetic events and ``session_col``.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        Parameters and details :py:func:`src.data_processors_lib.rete.split_sessions.SplitSessions`
        """

        # avoid circular import
        from src.data_processors_lib.rete import SplitSessions, SplitSessionsParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore
        params = dict(session_cutoff=session_cutoff, session_col=session_col, mark_truncated=mark_truncated)
        node = EventsNode(processor=SplitSessions(params=SplitSessionsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
