from __future__ import annotations

from typing import Literal, Optional

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class TruncatePathsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(  # type: ignore
        scope="truncate_paths",
        event_name="helper",
        event_value="combine",
    )
    def truncate_paths(
        self: EventstreamType,
        drop_before: Optional[str] = None,
        drop_after: Optional[str] = None,
        occurrence_before: Literal["first", "last"] = "first",
        occurrence_after: Literal["first", "last"] = "first",
        shift_before: int = 0,
        shift_after: int = 0,
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that truncates each user's path based on the
        specified event(s) and selected parameters.


        Parameters
        ----------
            %(TruncatePath.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with truncated paths.


        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            TruncatePaths,
            TruncatePathsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore
        params = {
            "drop_before": drop_before,
            "drop_after": drop_after,
            "occurrence_before": occurrence_before,
            "occurrence_after": occurrence_after,
            "shift_before": shift_before,
            "shift_after": shift_after,
        }
        not_hash_values = ["occurrence_before", "occurrence_after"]

        node = EventsNode(processor=TruncatePaths(params=TruncatePathsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="truncate_paths",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
