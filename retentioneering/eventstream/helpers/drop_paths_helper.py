from __future__ import annotations

from typing import Tuple

from retentioneering.constants import DATETIME_UNITS

from ..types import EventstreamType


class DropPathsHelperMixin:
    def drop_paths(
        self, min_steps: int | None = None, min_time: Tuple[float, DATETIME_UNITS] | None = None
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that deletes users' paths that are shorter than the specified
        number of events or cut_off.

        Parameters
        ----------
        See parameters description
            :py:class:`.DropPaths`

        Returns
        -------
        Eventstream
             Input ``eventstream`` without the deleted short users' paths.


        """

        # avoid circular import
        from retentioneering.data_processors_lib import DropPaths, DropPathsParams
        from retentioneering.preprocessing_graph.nodes import EventsNode
        from retentioneering.preprocessing_graph.preprocessing_graph import (
            PreprocessingGraph,
        )

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=DropPaths(params=DropPathsParams(min_steps=min_steps, min_time=min_time))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
