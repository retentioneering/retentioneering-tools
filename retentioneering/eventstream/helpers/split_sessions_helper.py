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


class SplitSessionsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="split_sessions",
        event_name="helper",
        event_value="combine",
    )
    def split_sessions(
        self: EventstreamType,
        timeout: Optional[Tuple[float, DATETIME_UNITS]] = None,
        delimiter_events: Optional[List[str]] = None,
        delimiter_col: Optional[str] = None,
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
            %(SplitSessions.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with new synthetic events and ``session_col``.

        """
        calling_params = {
            "timeout": timeout,
            "session_col": session_col,
            "mark_truncated": mark_truncated,
            "delimiter_events": delimiter_events,
            "delimiter_col": delimiter_col,
        }
        not_hash_values = ["timeout"]

        # avoid circular import
        from retentioneering.data_processors_lib import (
            SplitSessions,
            SplitSessionsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore
        params = dict(
            timeout=timeout,
            delimiter_events=delimiter_events,
            delimiter_col=delimiter_col,
            session_col=session_col,
            mark_truncated=mark_truncated,
        )
        node = EventsNode(processor=SplitSessions(params=SplitSessionsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="split_sessions",
            event_name="metadata",
            called_params=calling_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
