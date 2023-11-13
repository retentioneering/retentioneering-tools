from __future__ import annotations

from typing import Callable, Dict, List, Optional

from pandas import DataFrame, Series
from typing_extensions import (  # required for pydantic and python < 3.9.2
    NotRequired,
    Required,
    TypedDict,
)

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamSchemaType, EventstreamType

EventstreamFilter = Callable[[DataFrame, Optional[EventstreamSchemaType]], DataFrame]
GroupingRulesDict = Dict[str, EventstreamFilter]


class GroupEventsRule(TypedDict, total=False):
    event_name: Required[str]
    func: Required[EventstreamFilter]
    event_type: NotRequired[str]


class GroupEventsBulkHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(  # type: ignore
        scope="group_events_bulk",
        event_name="helper",
        event_value="combine",
    )
    def group_events_bulk(
        self: EventstreamType,
        grouping_rules: List[GroupEventsRule] | GroupingRulesDict,
        ignore_intersections: bool = False,
    ) -> EventstreamType:
        """
        Apply multiple grouping rules simultaneously.
        See also :py:meth:`GroupEvents<retentioneering.data_processors_lib.group_events.GroupEvents>`

        Parameters
        ----------
            %(GroupEventsBulk.parameters)s

        Returns
        -------
        Eventstream
            ``Eventstream`` with the grouped events according to the given grouping rules.

        """
        # avoid circular import
        from retentioneering.data_processors_lib import (
            GroupEventsBulk,
            GroupEventsBulkParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        params = GroupEventsBulkParams(grouping_rules=grouping_rules, ignore_intersections=ignore_intersections)  # type: ignore

        calling_params = params.dict()

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(processor=GroupEventsBulk(params=params))
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="map_events",
            event_name="metadata",
            called_params=calling_params,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
