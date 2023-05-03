from __future__ import annotations

from typing import Optional, Tuple

from retentioneering.backend.tracker import track
from retentioneering.constants import DATETIME_UNITS

from ..types import EventstreamType


class SplitSessionsHelperMixin:
    @track(  # type: ignore
        tracking_info={"event_name": "helper"},
        scope="split_sessions",
        event_value="combine",
        allowed_params=[
            "timeout",
            "session_col",
            "mark_truncated",
        ],
    )
    def split_sessions(
        self,
        timeout: Tuple[float, DATETIME_UNITS],
        session_col: str = "session_id",
        mark_truncated: Optional[bool] = False,
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates new synthetic events in each user's path:
        ``session_start`` (or ``session_start_cropped``) and ``session_end`` (or ``session_end_cropped``).
        The created events divide users' paths on sessions.
        Also creates a new column that contains session number for each event in the input eventstream
        Session number will take the form: ``{user_id}_{session_number through one user path}``.
        The created events and column are added to the input eventstream.

        Parameters
        ----------
        See parameters description
            :py:class:`.SplitSessions`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events and ``session_col``.

        """

        # avoid circular import
        from retentioneering.data_processors_lib import (
            SplitSessions,
            SplitSessionsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore
        params = dict(timeout=timeout, session_col=session_col, mark_truncated=mark_truncated)
        node = EventsNode(processor=SplitSessions(params=SplitSessionsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
