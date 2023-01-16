from __future__ import annotations

from typing import Literal, Optional

from ..types import EventstreamType


class TruncatePathHelperMixin:
    def truncate_path(
        self,
        drop_before: Optional[str] = None,
        drop_after: Optional[str] = None,
        occurrence_before: Literal["first", "last"] = "first",
        occurrence_after: Literal["first", "last"] = "first",
        shift_before: int = 0,
        shift_after: int = 0,
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which truncates each user's path on the base of
        specified event(s) and selected parameters.

        Returns
        -------
        Eventstream
             Input ``eventstream`` with truncated paths.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:func:`src.data_processors_lib.truncate_path.TruncatePath`
        """
        # avoid circular import
        from retentioneering.data_processors_lib import TruncatePath, TruncatePathParams
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore
        params = {
            "drop_before": drop_before,
            "drop_after": drop_after,
            "occurrence_before": occurrence_before,
            "occurrence_after": occurrence_after,
            "shift_before": shift_before,
            "shift_after": shift_after,
        }

        node = EventsNode(processor=TruncatePath(params=TruncatePathParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
