from __future__ import annotations

from typing import Tuple

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.constants import DATETIME_UNITS
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class DropPathsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="drop_paths",
        event_name="helper",
        event_value="combine",
    )
    def drop_paths(
        self: EventstreamType, min_steps: int | None = None, min_time: Tuple[float, DATETIME_UNITS] | None = None
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that deletes users' paths that are shorter than the specified
        number of events or cut_off.

        Parameters
        ----------
            %(DropPaths.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` without the deleted short users' paths.


        """
        calling_params = {
            "min_steps": min_steps,
            "min_time": min_time,
        }
        not_hash_values = ["min_time"]

        # avoid circular import
        from retentioneering.data_processors_lib import DropPaths, DropPathsParams
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=DropPaths(params=DropPathsParams(min_steps=min_steps, min_time=min_time))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="drop_paths",
            event_name="metadata",
            called_params=calling_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
