from __future__ import annotations

from typing import Callable, Optional

from pandas import DataFrame, Series

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamSchemaType, EventstreamType


class PipeHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(  # type: ignore
        scope="pipe",
        event_name="helper",
        event_value="combine",
    )
    def pipe(
        self: EventstreamType, func: Callable[[DataFrame, Optional[EventstreamSchemaType]], DataFrame]
    ) -> EventstreamType:
        """
        Modify an input eventstream in an arbitrary way by applying given function.
        The function must accept a DataFrame associated with the input eventstream
        and return a new state of the modified eventstream.

        Parameters
        ----------
            %(Pipe.parameters)s

        Returns
        -------
        Eventstream
            Resulting eventstream
        """
        calling_params = {
            "func": func,
        }

        # avoid circular import
        from retentioneering.data_processors_lib import Pipe, PipeParams
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=Pipe(params=PipeParams(func=func)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="pipe",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
