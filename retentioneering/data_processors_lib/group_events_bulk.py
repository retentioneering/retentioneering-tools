from inspect import signature
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import EnumWidget

EventstreamFilter = Callable[[pd.DataFrame, Optional[EventstreamSchemaType]], Any]

GroupingRulesDict = Dict[str, EventstreamFilter]


@dataclass
class GroupEventsRule:
    event_name: str
    func: EventstreamFilter
    event_type: Optional[str] = None


class GroupEventsBulkParams(ParamsModel):
    grouping_rules: Union[List[GroupEventsRule], GroupingRulesDict]
    ignore_intersections: bool = False

    _widgets = {
        # @TODO: is stub for editor, fix later
        "grouping_rules": EnumWidget(),
    }


def combine_masks(masks: List[pd.Series]) -> pd.Series:
    mask_arrays = [mask.values for mask in masks]
    combined_mask = np.sum(mask_arrays, axis=0) > 1  # type: ignore
    result_mask = pd.Series(combined_mask, index=masks[0].index)
    return result_mask


@docstrings.get_sections(base="GroupEventsBulk")  # type: ignore
class GroupEventsBulk(DataProcessor):
    """
    Apply multiple grouping rules simultaneously.
    See also :py:meth:`GroupEvents<retentioneering.data_processors_lib.group_events.GroupEvents>`

    Parameters
    ----------
    grouping_rules : list or dict
        - If list, each list element is a dictionary with mandatory keys ``event_name`` and ``func`` and an
          optional key ``event_type``. Their meaning is the same as for
          :py:meth:`GroupEvents<retentioneering.data_processors_lib.group_events.GroupEvents>`.
        - If dict, the keys are considered as ``event_name``, values are considered as ``func``.
          Setting ``event_type`` is not supported in this case.

    ignore_intersections : bool, default False
        If ``False``, a ``ValueError`` is raised in case any event from the input eventstream matches
        more than one grouping rule. Otherwise, the first appropriate rule from ``grouping_rules`` is applied.

    Returns
    -------
    Eventstream
        ``Eventstream`` with the grouped events according to the given grouping rules.

    """

    params: GroupEventsBulkParams

    @time_performance(
        scope="group_events_bulk",
        event_name="init",
    )
    def __init__(self, params: GroupEventsBulkParams) -> None:
        super().__init__(params=params)

    @time_performance(
        scope="group_events_bulk",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        rules = self.params.grouping_rules
        ignore_intersections = self.params.ignore_intersections

        if isinstance(rules, dict):
            rules_list: List[GroupEventsRule] = []
            for key, val in rules.items():
                rules_list.append(GroupEventsRule(event_name=key, func=val))  # type: ignore
            rules = rules_list

        parent_info = {
            "shape": df.shape,
            "hash": hash_dataframe(df),
        }

        masks: List[pd.Series] = []
        source = df.copy()

        for rule in rules:
            event_name = rule.event_name
            func: Callable = rule.func
            event_type = rule.event_type if rule.event_type else "group_alias"

            expected_args_count = len(signature(func).parameters)
            if expected_args_count == 1:
                mask = func(df)  # type: ignore
                if not ignore_intersections:
                    source_mask = func(source)  # type: ignore
                    masks.append(source_mask)
            else:
                mask = func(df, schema)
                if not ignore_intersections:
                    source_mask = func(source, schema)
                    masks.append(source_mask)

            with pd.option_context("mode.chained_assignment", None):
                df.loc[mask, schema.event_type] = event_type
                df.loc[mask, schema.event_name] = event_name

        if not ignore_intersections:
            intersection_mask = combine_masks(masks)
            has_intersections = intersection_mask.any()

            if has_intersections:
                raise ValueError(
                    "GroupEventsBulk Dataprocessor error. Mapping rules are intersected. Use ignore_intersections=True or fix the intersections"
                )

        collect_data_performance(
            scope="group_events_bulk",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            performance_data={
                "parent": parent_info,
                "child": {
                    "shape": df.shape,
                    "hash": hash_dataframe(df),
                },
            },
        )

        return df
