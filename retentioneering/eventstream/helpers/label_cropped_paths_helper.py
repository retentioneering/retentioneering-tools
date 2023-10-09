from __future__ import annotations

from typing import Optional, Tuple

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.constants import DATETIME_UNITS
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class LabelCroppedPathsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="label_cropped_paths",
        event_name="helper",
        event_value="combine",
    )
    def label_cropped_paths(
        self: EventstreamType,
        left_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
        right_cutoff: Optional[Tuple[float, DATETIME_UNITS]],
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that creates new synthetic event(s) for each user based
        on the timeout threshold: ``cropped_left`` and ``cropped_right``.


        Parameters
        ----------
            %(LabelCroppedPaths.parameters)s

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.


        """
        calling_params = {
            "left_cutoff": left_cutoff,
            "right_cutoff": right_cutoff,
        }
        not_hash_values = ["left_cutoff", "right_cutoff"]

        # avoid circular import
        from retentioneering.data_processors_lib import (
            LabelCroppedPaths,
            LabelCroppedPathsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        params = dict(left_cutoff=left_cutoff, right_cutoff=right_cutoff)

        node = EventsNode(processor=LabelCroppedPaths(params=LabelCroppedPathsParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="label_cropped_paths",
            event_name="metadata",
            called_params=calling_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
