# flake8: noqa
from __future__ import annotations

import uuid
from collections.abc import Collection
from typing import Any, Callable, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
from IPython.display import display
from matplotlib.axes import SubplotBase

from src.constants import DATETIME_UNITS
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.eventstream.types import EventstreamType, RawDataSchemaType, Relation
from src.graph import PGraph, SourceNode
from src.tooling.clusters import Clusters
from src.tooling.cohorts import Cohorts
from src.tooling.funnel import Funnel
from src.tooling.stattests import TEST_NAMES, StatTests
from src.tooling.step_matrix import StepMatrix
from src.tooling.step_sankey import StepSankey
from src.tooling.timedelta_hist import AGGREGATION_NAMES, TimedeltaHist
from src.tooling.user_lifetime_hist import UserLifetimeHist
from src.transition_graph import NormType, TransitionGraph
from src.transition_graph.typing import Threshold
from src.utils import get_merged_col
from src.utils.list import find_index

from .helpers import (
    CollapseLoopsHelperMixin,
    DeleteUsersByPathLengthHelperMixin,
    FilterHelperMixin,
    GroupHelperMixin,
    LostUsersHelperMixin,
    NegativeTargetHelperMixin,
    NewUsersHelperMixin,
    PositiveTargetHelperMixin,
    RenameHelperMixin,
    SplitSessionsHelperMixin,
    StartEndHelperMixin,
    TruncatedEventsHelperMixin,
    TruncatePathHelperMixin,
)

IndexOrder = List[Optional[str]]
FeatureType = Literal["tfidf", "count", "frequency", "binary", "time", "time_fraction", "external"]
NgramRange = Tuple[int, int]
Method = Literal["kmeans", "gmm"]


DEFAULT_INDEX_ORDER: IndexOrder = [
    "profile",
    "path_start",
    "new_user",
    "existing_user",
    "truncated_left",
    "session_start",
    "session_start_truncated",
    "group_alias",
    "raw",
    "raw_sleep",
    None,
    "synthetic",
    "synthetic_sleep",
    "positive_target",
    "negative_target",
    "session_end_truncated",
    "session_end",
    "session_sleep",
    "truncated_right",
    "absent_user",
    "lost_user",
    "path_end",
]

RAW_COL_PREFIX = "raw_"
DELETE_COL_NAME = "_deleted"


# TODO проработать резервирование колонок


class Eventstream(
    CollapseLoopsHelperMixin,
    DeleteUsersByPathLengthHelperMixin,
    FilterHelperMixin,
    GroupHelperMixin,
    LostUsersHelperMixin,
    NegativeTargetHelperMixin,
    NewUsersHelperMixin,
    PositiveTargetHelperMixin,
    SplitSessionsHelperMixin,
    StartEndHelperMixin,
    TruncatedEventsHelperMixin,
    TruncatePathHelperMixin,
    RenameHelperMixin,
    EventstreamType,
):
    """
    Collection of tools for storing and processing clickstream data.

    Parameters
    ----------
    raw_data : pd.DataFrame or pd.Series
        Raw clickstream data.
    raw_data_schema : RawDataSchema, optional
        Should be specified as an instance of class ``RawDataSchema``:

        - If ``raw_data`` column names are different from default :py:func:`src.eventstream.schema.RawDataSchema`.
        - If there is at least one ``custom_col`` in ``raw_data``.

    schema : EventstreamSchema, optional
        Schema of created ``eventstream``.
        See default schema% :py:func:`src.eventstream.schema.EventstreamSchema`.
    prepare : bool, default True

        - If ``True`` input data will be transformed in the following way:
            - Convert column ``event_timestamp`` to pandas datetime format.
            - | Adds ``event_type`` column and fills with ``raw`` value.
              | if that column already exists it will remain unchanged.
        - If ``False`` - ``raw_data`` will be remained as is.

    index_order : list of str, default DEFAULT_INDEX_ORDER
        Sorting order for ``event_type`` column.
    relations : list, optional
    user_sample_size : int of float, optional
        Number (``int``) or share (``float``) of all users trajectories which will be randomly chosen
        and remained in final sample.
        See :numpy_random_choice:`numpy documentation<>`.
    user_sample_seed : int, optional
        Random seed value to generate repeated random users sample.
        See :numpy_random_seed:`numpy documentation<>`.

    """

    schema: EventstreamSchema
    index_order: IndexOrder
    relations: List[Relation]
    __raw_data_schema: RawDataSchemaType
    __events: pd.DataFrame | pd.Series[Any]
    __clusters: Clusters | None = None
    __funnel: Funnel | None = None
    __cohorts: Cohorts | None = None
    __step_matrix: StepMatrix | None = None
    __sankey: StepSankey | None = None
    __stattests: StatTests | None = None
    __timedelta_hist: TimedeltaHist | None = None
    __user_lifetime_hist: UserLifetimeHist | None = None
    __transition_graph: TransitionGraph | None = None
    __p_graph: PGraph | None = None

    def __init__(
        self,
        raw_data: pd.DataFrame | pd.Series[Any],
        raw_data_schema: RawDataSchemaType | None = None,
        schema: EventstreamSchema | None = None,
        prepare: bool = True,
        index_order: Optional[IndexOrder] = None,
        relations: Optional[List[Relation]] = None,
        user_sample_size: Optional[int | float] = None,
        user_sample_seed: Optional[int] = None,
    ) -> None:
        self.__clusters = None
        self.__funnel = None
        self.schema = schema if schema else EventstreamSchema()

        if not raw_data_schema:
            raw_data_schema = RawDataSchema()
            if "event_type" in raw_data.columns:
                raw_data_schema.event_type = "event_type"
        self.__raw_data_schema = raw_data_schema

        if user_sample_size is not None:
            raw_data = self.__sample_user_paths(raw_data, raw_data_schema, user_sample_size, user_sample_seed)
        if not index_order:
            self.index_order = DEFAULT_INDEX_ORDER
        else:
            self.index_order = index_order
        if not relations:
            self.relations = []
        else:
            self.relations = relations
        self.__events = self.__prepare_events(raw_data) if prepare else raw_data
        self.index_events()

    def copy(self) -> Eventstream:
        """
        Make a copy of current ``eventstream``.

        Returns
        -------
        Eventstream

        """
        return Eventstream(
            raw_data_schema=self.__raw_data_schema.copy(),
            raw_data=self.__events.copy(),
            schema=self.schema.copy(),
            prepare=False,
            index_order=self.index_order.copy(),
            relations=self.relations.copy(),
        )

    def append_eventstream(self, eventstream: Eventstream) -> None:  # type: ignore
        """
        Append ``eventstream`` with the same schema.

        Parameters
        ----------
        eventstream : Eventstream

        Returns
        -------
        eventstream

        Raises
        ------
        ValueError
            If ``EventstreamSchemas`` of two ``eventstreams`` are not equal.
        """
        if not self.schema.is_equal(eventstream.schema):
            raise ValueError("invalid schema: joined eventstream")

        curr_events = self.to_dataframe(raw_cols=True, show_deleted=True)
        new_events = eventstream.to_dataframe(raw_cols=True, show_deleted=True)

        curr_deleted_events = curr_events[curr_events[DELETE_COL_NAME] == True]
        new_deleted_events = new_events[new_events[DELETE_COL_NAME] == True]
        deleted_events = pd.concat([curr_deleted_events, new_deleted_events])
        deleted_events = deleted_events.drop_duplicates(subset=[self.schema.event_id])

        merged_events = pd.merge(
            curr_events,
            new_events,
            left_on=self.schema.event_id,
            right_on=self.schema.event_id,
            how="outer",
            indicator=True,
        )

        left_events = merged_events[(merged_events["_merge"] == "left_only") | (merged_events["_merge"] == "both")]
        right_events = merged_events[(merged_events["_merge"] == "right_only")]

        left_raw_cols = self._get_raw_cols()
        right_raw_cols = eventstream._get_raw_cols()
        cols = self.schema.get_cols()

        result_left_part = pd.DataFrame()
        result_right_part = pd.DataFrame()

        for col in cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        for col in left_raw_cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")

        for col in right_raw_cols:
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        result_left_part[DELETE_COL_NAME] = get_merged_col(df=left_events, colname=DELETE_COL_NAME, suffix="_x")
        result_right_part[DELETE_COL_NAME] = get_merged_col(df=right_events, colname=DELETE_COL_NAME, suffix="_y")

        self.__events = pd.concat([result_left_part, result_right_part])
        self._soft_delete(deleted_events)
        self.index_events()

    def _join_eventstream(self, eventstream: Eventstream) -> None:  # type: ignore
        if not self.schema.is_equal(eventstream.schema):
            raise ValueError("invalid schema: joined eventstream")

        relation_i = find_index(
            input_list=eventstream.relations,
            cond=lambda rel: rel["eventstream"] == self,
        )

        if relation_i == -1:
            raise ValueError("relation not found!")

        relation_col_name = f"ref_{relation_i}"

        curr_events = self.to_dataframe(raw_cols=True, show_deleted=True)
        joined_events = eventstream.to_dataframe(raw_cols=True, show_deleted=True)
        not_related_events = joined_events[joined_events[relation_col_name].isna()]
        not_related_events_ids = not_related_events[self.schema.event_id]

        merged_events = pd.merge(
            curr_events,
            joined_events,
            left_on=self.schema.event_id,
            right_on=relation_col_name,
            how="outer",
            indicator=True,
        )

        left_id_colname = f"{self.schema.event_id}_y"

        both_events = merged_events[(merged_events["_merge"] == "both")]
        left_events = merged_events[(merged_events["_merge"] == "left_only")]
        right_events = merged_events[
            (merged_events["_merge"] == "both") | (merged_events[left_id_colname].isin(not_related_events_ids))
        ]

        left_raw_cols = self._get_raw_cols()
        right_raw_cols = eventstream._get_raw_cols()
        cols = self._get_both_cols(eventstream)

        result_left_part = pd.DataFrame()
        result_right_part = pd.DataFrame()
        result_deleted_events = pd.DataFrame()

        for col in cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_deleted_events[col] = get_merged_col(df=both_events, colname=col, suffix="_x")
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        for col in left_raw_cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_deleted_events[col] = get_merged_col(df=both_events, colname=col, suffix="_x")

        for col in right_raw_cols:
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        result_left_part[DELETE_COL_NAME] = get_merged_col(df=left_events, colname=DELETE_COL_NAME, suffix="_x")

        result_deleted_events[DELETE_COL_NAME] = True

        left_delete_col = f"{DELETE_COL_NAME}_x"
        right_delete_col = f"{DELETE_COL_NAME}_y"
        result_right_part[DELETE_COL_NAME] = right_events[left_delete_col] | right_events[right_delete_col]

        self.__events = pd.concat([result_left_part, result_right_part, result_deleted_events])
        self.schema.custom_cols = self._get_both_custom_cols(eventstream)
        self.index_events()

    def _get_both_custom_cols(self, eventstream: Eventstream) -> list[str]:
        self_custom_cols = set(self.schema.custom_cols)
        eventstream_custom_cols = set(eventstream.schema.custom_cols)
        all_custom_cols = self_custom_cols.union(eventstream_custom_cols)
        return list(all_custom_cols)

    def _get_both_cols(self, eventstream: Eventstream) -> list[str]:
        self_cols = set(self.schema.get_cols())
        eventstream_cols = set(eventstream.schema.get_cols())
        all_cols = self_cols.union(eventstream_cols)
        return list(all_cols)

    def to_dataframe(self, raw_cols: bool = False, show_deleted: bool = False, copy: bool = False) -> pd.DataFrame:
        """
        Convert ``eventstream`` to ``pd.Dataframe``

        Parameters
        ----------
        raw_cols : bool, default False
            If ``True`` - original columns of the input ``raw_data`` will be shown.
        show_deleted : bool, default
            If ``True`` - show all rows in ``eventstream``
        copy : bool, default False
            If ``True`` - copy data from current ``eventstream``.
            See details :pandas_copy:`pandas documentation<>`

        Returns
        -------
        pd.DataFrame

        """
        cols = self.schema.get_cols() + self._get_relation_cols()

        if raw_cols:
            cols += self._get_raw_cols()
        if show_deleted:
            cols.append(DELETE_COL_NAME)

        events = self.__events if show_deleted else self.__get_not_deleted_events()
        view = pd.DataFrame(events, columns=cols, copy=copy)
        return view

    def index_events(self) -> None:
        """
        Sort and index eventstream using DEFAULT_INDEX_ORDER.

        Returns
        -------
        None

        """
        order_temp_col_name = "order"
        indexed = self.__events

        indexed[order_temp_col_name] = indexed[self.schema.event_type].apply(lambda e: self.__get_event_priority(e))
        indexed = indexed.sort_values([self.schema.event_timestamp, order_temp_col_name])  # type: ignore
        indexed = indexed.drop([order_temp_col_name], axis=1)
        # indexed[id_col_col_name] = range(1, len(indexed) + 1)
        indexed.reset_index(inplace=True, drop=True)
        indexed[self.schema.event_index] = indexed.index
        self.__events = indexed

    def _get_raw_cols(self) -> list[str]:
        cols = self.__events.columns
        raw_cols: list[str] = []
        for col in cols:
            if col.startswith(RAW_COL_PREFIX):
                raw_cols.append(col)
        return raw_cols

    def _get_relation_cols(self) -> list[str]:
        cols = self.__events.columns
        relation_cols: list[str] = []
        for col in cols:
            if col.startswith("ref_"):
                relation_cols.append(col)
        return relation_cols

    def add_custom_col(self, name: str, data: pd.Series[Any] | None) -> None:
        """
        Add custom column to existing ``eventstream``.

        Parameters
        ----------
        name : str
            New column name.
        data : pd.Series

            - If ``pd.Series`` - new column with given values will be added.
            - If ``None`` - new column will be filled with ``np.nan``

        Returns
        -------
        Eventstream
        """
        self.__raw_data_schema.custom_cols.extend([{"custom_col": name, "raw_data_col": name}])
        self.schema.custom_cols.extend([name])
        self.__events[name] = data

    def _soft_delete(self, events: pd.DataFrame) -> None:
        """
        Method deletes events either by event_id or by the last relation.
        """
        deleted_events = events.copy()
        deleted_events[DELETE_COL_NAME] = True
        merged = pd.merge(
            left=self.__events,
            right=deleted_events,
            left_on=self.schema.event_id,
            right_on=self.schema.event_id,
            indicator=True,
            how="left",
        )
        if relation_cols := self._get_relation_cols():
            last_relation_col = relation_cols[-1]
            self.__events[DELETE_COL_NAME] = self.__events[DELETE_COL_NAME] | merged[f"{DELETE_COL_NAME}_y"] == True
            merged = pd.merge(
                left=self.__events,
                right=deleted_events,
                left_on=last_relation_col,
                right_on=self.schema.event_id,
                indicator=True,
                how="left",
            )

        self.__events[DELETE_COL_NAME] = self.__events[DELETE_COL_NAME] | merged[f"{DELETE_COL_NAME}_y"] == True

    def __get_not_deleted_events(self) -> pd.DataFrame | pd.Series[Any]:
        events = self.__events
        return events[events[DELETE_COL_NAME] == False]

    def __prepare_events(self, raw_data: pd.DataFrame | pd.Series[Any]) -> pd.DataFrame | pd.Series[Any]:
        events = raw_data.copy()
        # add "raw_" prefix for raw cols
        events.rename(lambda col: f"raw_{col}", axis="columns", inplace=True)

        events[DELETE_COL_NAME] = False
        events[self.schema.event_id] = [uuid.uuid4() for x in range(len(events))]
        events[self.schema.event_name] = self.__get_col_from_raw_data(
            raw_data=raw_data,
            colname=self.__raw_data_schema.event_name,
        )
        events[self.schema.event_timestamp] = pd.to_datetime(
            self.__get_col_from_raw_data(
                raw_data=raw_data,
                colname=self.__raw_data_schema.event_timestamp,
            ),
        )
        events[self.schema.user_id] = self.__get_col_from_raw_data(
            raw_data=raw_data,
            colname=self.__raw_data_schema.user_id,
        )

        if self.__raw_data_schema.event_type is not None:
            events[self.schema.event_type] = self.__get_col_from_raw_data(
                raw_data=raw_data,
                colname=self.__raw_data_schema.event_type,
            )
        else:
            events[self.schema.event_type] = "raw"

        for custom_col_schema in self.__raw_data_schema.custom_cols:
            raw_data_col = custom_col_schema["raw_data_col"]
            custom_col = custom_col_schema["custom_col"]
            if custom_col not in self.schema.custom_cols:
                self.schema.custom_cols.append(custom_col)

            events[custom_col] = self.__get_col_from_raw_data(
                raw_data=raw_data,
                colname=raw_data_col,
            )

        for custom_col in self.schema.custom_cols:
            if custom_col in events.columns:
                continue
            events[custom_col] = np.nan

        # add relations
        for i in range(len(self.relations)):
            rel_col_name = f"ref_{i}"
            relation = self.relations[i]
            col = raw_data[relation["raw_col"]] if relation["raw_col"] is not None else np.nan
            events[rel_col_name] = col

        return events

    def __get_col_from_raw_data(
        self, raw_data: pd.DataFrame | pd.Series[Any], colname: str, create: bool = False
    ) -> pd.Series | float:
        if colname in raw_data.columns:
            return raw_data[colname]
        else:
            if create:
                return np.nan
            else:
                raise ValueError(f'invalid raw data. Column "{colname}" does not exists!')

    def __get_event_priority(self, event_type: Optional[str]) -> int:
        if event_type in self.index_order:
            return self.index_order.index(event_type)
        return len(self.index_order)

    def __sample_user_paths(
        self,
        raw_data: pd.DataFrame | pd.Series[Any],
        raw_data_schema: RawDataSchemaType,
        user_sample_size: Optional[int | float] = None,
        user_sample_seed: Optional[int] = None,
    ) -> pd.DataFrame | pd.Series[Any]:
        if type(user_sample_size) is not float and type(user_sample_size) is not int:
            raise TypeError('"user_sample_size" has to be a number(float for user share or int for user amount)')
        if user_sample_size < 0:
            raise ValueError("User sample size/share cannot be negative!")
        if type(user_sample_size) is float:
            if user_sample_size > 1:
                raise ValueError("User sample share cannot exceed 1!")
        user_col_name = raw_data_schema.user_id
        unique_users = raw_data[user_col_name].unique()
        if type(user_sample_size) is int:
            sample_size = user_sample_size
        elif type(user_sample_size) is float:
            sample_size = int(user_sample_size * len(unique_users))
        else:
            return raw_data
        if user_sample_seed is not None:
            np.random.seed(user_sample_seed)
        sample_users = np.random.choice(unique_users, sample_size, replace=False)
        raw_data_sampled = raw_data.loc[raw_data[user_col_name].isin(sample_users), :]  # type: ignore
        return raw_data_sampled

    def funnel(
        self,
        stages: list[str],
        stage_names: list[str] | None = None,
        funnel_type: Literal["open", "closed"] = "open",
        segments: Collection[Collection[int]] | None = None,
        segment_names: list[str] | None = None,
        sequence: bool = False,
        show_plot: bool = True,
    ) -> Funnel:

        """
        Shows a visualization of the user sequential events represented as a funnel.

        See parameters description :py:func:`src.tooling.funnel.funnel`

        Returns
        -------
        Funnel
            A ``Funnel`` class instance fitted to the given parameters.

        """
        self.__funnel = Funnel(
            eventstream=self,
            stages=stages,
            stage_names=stage_names,
            funnel_type=funnel_type,
            segments=segments,
            segment_names=segment_names,
            sequence=sequence,
        )
        self.__funnel.fit()
        if show_plot:
            figure = self.__funnel.plot()
            figure.show()
        return self.__funnel

    @property
    def clusters(self) -> Clusters:
        """
        Returns an instance of ``Clusters`` class to be used for cluster analysis.

        See :py:func:`src.tooling.clusters.clusters`

        Returns
        -------
        Clusters
        """
        if self.__clusters is None:
            self.__clusters = Clusters(eventstream=self)
        return self.__clusters

    def step_matrix(
        self,
        max_steps: int = 20,
        weight_col: Optional[str] = None,
        precision: int = 2,
        targets: Optional[list[str] | str] = None,
        accumulated: Optional[Union[Literal["both", "only"], None]] = None,
        sorting: Optional[list[str]] = None,
        thresh: float = 0,
        centered: Optional[dict] = None,
        groups: Optional[Tuple[list, list]] = None,
        show_plot: bool = True,
    ) -> StepMatrix:
        """
        Shows a heatmap visualization of the step matrix.

        See parameters description :py:func:`src.tooling.step_matrix.step_matrix`

        Returns
        -------
        StepMatrix
            A ``StepMatrix`` class instance fitted to the given parameters.

        """
        self.__step_matrix = StepMatrix(
            eventstream=self,
            max_steps=max_steps,
            weight_col=weight_col,
            precision=precision,
            targets=targets,
            accumulated=accumulated,
            sorting=sorting,
            thresh=thresh,
            centered=centered,
            groups=groups,
        )

        self.__step_matrix.fit()
        if show_plot:
            figure = self.__step_matrix.plot()
            figure.show()
        return self.__step_matrix

    def step_sankey(
        self,
        max_steps: int = 10,
        thresh: Union[int, float] = 0.05,
        sorting: list | None = None,
        target: Union[list[str], str] | None = None,
        autosize: bool = True,
        width: int | None = None,
        height: int | None = None,
        show_plot: bool = True,
    ) -> StepSankey:
        """
        Shows a Sankey diagram visualizing the user paths in step-wise manner.

        See parameters description :py:func:`src.tooling.step_sankey.step_sankey`

        Returns
        -------
        StepSankey
            A ``StepSankey`` class instance fitted to the given parameters.

        """
        self.__sankey = StepSankey(
            eventstream=self,
            max_steps=max_steps,
            thresh=thresh,
            sorting=sorting,
            target=target,
            autosize=autosize,
            width=width,
            height=height,
        )

        self.__sankey.fit()
        if show_plot:
            figure = self.__sankey.plot()
            figure.show()
        return self.__sankey

    def cohorts(
        self,
        cohort_start_unit: DATETIME_UNITS,
        cohort_period: Tuple[int, DATETIME_UNITS],
        average: bool = True,
        cut_bottom: int = 0,
        cut_right: int = 0,
        cut_diagonal: int = 0,
        figsize: Tuple[float, float] = (10, 10),
        show_plot: bool = True,
    ) -> Cohorts:

        """
        Shows a heatmap visualization of the user appearance grouped by cohorts.

        See parameters description :py:func:`src.tooling.cohorts.cohorts`

        Returns
        -------
        Cohorts
            A ``Cohorts`` class instance fitted to the given parameters.
        """

        self.__cohorts = Cohorts(
            eventstream=self,
            cohort_start_unit=cohort_start_unit,
            cohort_period=cohort_period,
            average=average,
            cut_bottom=cut_bottom,
            cut_right=cut_right,
            cut_diagonal=cut_diagonal,
        )

        self.__cohorts.fit()
        if show_plot:
            self.__cohorts.heatmap(figsize)
        return self.__cohorts

    def stattests(
        self,
        test: TEST_NAMES,
        groups: Tuple[list[str | int], list[str | int]],
        function: Callable,
        group_names: Tuple[str, str] = ("group_1", "group_2"),
        alpha: float = 0.05,
    ) -> StatTests:
        """
        Determines the statistical difference between the metric values in two user groups.

        See parameters description :py:func:`src.tooling.stattests.stattests`

        Returns
        -------
        StatTests
            A ``StatTest`` class instance fitted to the given parameters.
        """
        self.__stattests = StatTests(
            eventstream=self, groups=groups, func=function, test=test, group_names=group_names, alpha=alpha
        )
        self.__stattests.fit()
        values = self.__stattests.values
        str_template = "{0} (mean ± SD): {1:.3f} ± {2:.3f}, n = {3}"

        print(
            str_template.format(
                values["group_one_name"], values["group_one_mean"], values["group_one_std"], values["group_one_size"]
            )
        )
        print(
            str_template.format(
                values["group_two_name"], values["group_two_mean"], values["group_two_std"], values["group_two_size"]
            )
        )
        print(
            "'{0}' is greater than '{1}' with P-value: {2:.5f}".format(
                values["greatest_group_name"], values["least_group_name"], values["p_val"]
            )
        )
        print("power of the test: {0:.2f}%".format(100 * values["power_estimated"]))

        return self.__stattests

    def timedelta_hist(
        self,
        event_pair: Optional[Tuple[str, str] | List[str]] = None,
        only_adjacent_event_pairs: bool = True,
        weight_col: Optional[str] = None,
        aggregation: Optional[AGGREGATION_NAMES] = None,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> TimedeltaHist:
        """
        Plots the distribution of the time deltas between two events. Supports various
        distribution types, such as distribution of time for adjacent consecutive events, or
        for a pair of pre-defined events, or median transition time from event to event per user/session.

        See parameters description :py:func:`src.tooling.timedelta_hist.timedelta_hist`

        Returns
        -------
        TimedeltaHist
            A ``TimedeltaHist`` class instance fitted to the given parameters.
        """
        self.__timedelta_hist = TimedeltaHist(
            eventstream=self,
            event_pair=event_pair,
            only_adjacent_event_pairs=only_adjacent_event_pairs,
            aggregation=aggregation,
            weight_col=weight_col,
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
        )
        return self.__timedelta_hist

    def user_lifetime_hist(
        self,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> UserLifetimeHist:
        """
        Plots the distribution of user lifetimes. A users' lifetime is the timedelta between the first and the last
        events of the user. Can be useful for finding suitable parameters of various data processors, such as
        DeleteUsersByPathLength or TruncatedEvents.

        See parameters description :py:func:`src.tooling.timedelta_hist.timedelta_hist`

        Returns
        -------
        UserLifetimeHist
            A ``UserLifetimeHist`` class instance fitted to the given parameters.
        """
        self.__user_lifetime_hist = UserLifetimeHist(
            eventstream=self,
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
        )
        return self.__user_lifetime_hist

    def event_timestamp_hist(
        self,
        event_list: Optional[List[str] | str] = "all",
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> SubplotBase:
        """
        Plots the distribution of events over time. Can be useful for detecting time-based anomalies, and visualising
        general timespan of the eventrtream.

        Returns
        -------
        SubplotBase
            A ``SubplotBase`` class instance fitted to the given parameters.
        """
        if lower_cutoff_quantile is not None:
            if not 0 < lower_cutoff_quantile < 1:
                raise ValueError("lower_cutoff_quantile should be a fraction between 0 and 1.")
        if upper_cutoff_quantile is not None:
            if not 0 < upper_cutoff_quantile < 1:
                raise ValueError("upper_cutoff_quantile should be a fraction between 0 and 1.")

        data = self.to_dataframe()

        if event_list != "all":
            if type(event_list) is not list:
                raise TypeError('event_list should either be "all", or a list of event names to include.')
            data = data[data[self.schema.event_name].isin(event_list)]

        values = data[self.schema.event_timestamp]
        idx = [True] * len(values)
        if upper_cutoff_quantile is not None:
            idx &= values <= values.quantile(upper_cutoff_quantile)
        if lower_cutoff_quantile is not None:
            idx &= values >= values.quantile(lower_cutoff_quantile)
        return values[idx].hist(bins=bins)

    def describe(self, session_col: Optional[str] = "session_id") -> None:
        """
        Displays general eventstream information. If session_col is present in eventstream columns, also
        outputs session statistics, assuming session_col is the session identifier column.
        """
        user_col, event_col, time_col, type_col = (
            self.schema.user_id,
            self.schema.event_name,
            self.schema.event_timestamp,
            self.schema.event_type,
        )

        df = self.to_dataframe()
        has_sessions = session_col in df.columns

        df = df[df[type_col].isin(["raw"])]
        max_time = df[time_col].max()
        min_time = df[time_col].min()

        print("----------------------------------------------------------------------------")
        print("\033[1mBasic statistics\033[0m")
        print()

        if has_sessions:
            out_df = pd.DataFrame(
                [
                    ["unique users", df[user_col].nunique()],
                    ["unique events", df[event_col].nunique()],
                    ["unique sessions", df[session_col].nunique()],  # type: ignore
                    ["eventstream start", df[time_col].min()],
                    ["eventstream end", df[time_col].max()],
                    ["eventstream length", max_time - min_time],
                ],
                columns=["metric", "value"],
            ).set_index("metric")
        else:
            out_df = pd.DataFrame(
                [
                    ["unique users", df[user_col].nunique()],
                    ["unique events", df[event_col].nunique()],
                    ["eventstream start", df[time_col].min()],
                    ["eventstream end", df[time_col].max()],
                    ["eventstream length", max_time - min_time],
                ],
                columns=["metric", "value"],
            ).set_index("metric")
        display(out_df)

        user_agg = df.groupby(user_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
        time_diff_user = user_agg[(time_col, "max")] - user_agg[(time_col, "min")]
        mean_time_user = time_diff_user.mean()
        median_time_user = time_diff_user.median()
        std_time_user = time_diff_user.std()
        min_length_time_user = time_diff_user.min()
        max_length_time_user = time_diff_user.max()

        print("----------------------------------------------------------------------------")
        session_agg = None
        if has_sessions:
            session_agg = df.groupby(session_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
            time_diff_session = session_agg[(time_col, "max")] - session_agg[(time_col, "min")]
            mean_time_session = time_diff_session.mean()
            median_time_session = time_diff_session.median()
            std_time_session = time_diff_session.std()
            min_length_time_session = time_diff_session.min()
            max_length_time_session = time_diff_session.max()

            print("\033[1mUser path/session time length\033[0m")
            out_df = pd.DataFrame(
                [
                    ["mean time", mean_time_user, mean_time_session],
                    ["std time", std_time_user, std_time_session],
                    ["median time", median_time_user, median_time_session],
                    ["min time", min_length_time_user, min_length_time_session],
                    ["max time", max_length_time_user, max_length_time_session],
                ],
                columns=["metric", "per user path", "per session"],
            ).set_index("metric")
        else:
            print("\033[1mUser path time length\033[0m")
            out_df = pd.DataFrame(
                [
                    ["mean time", mean_time_user],
                    ["std time", std_time_user],
                    ["median time", median_time_user],
                    ["min time", min_length_time_user],
                    ["max time", max_length_time_user],
                ],
                columns=["metric", "per user path"],
            ).set_index("metric")
        display(out_df)

        # events
        event_count_user = user_agg[(event_col, "count")]
        mean_user = round(event_count_user.mean(), 2)  # type: ignore
        median_user = event_count_user.median()
        std_user = round(event_count_user.std(), 2)  # type: ignore
        min_length_user = event_count_user.min()
        max_length_user = event_count_user.max()

        print("----------------------------------------------------------------------------")
        if has_sessions:
            assert session_agg is not None
            event_count_session = session_agg[(event_col, "count")]
            mean_session = round(event_count_session.mean(), 2)  # type: ignore
            median_session = event_count_session.median()
            std_session = round(event_count_session.std(), 2)  # type: ignore
            min_length_session = event_count_session.min()
            max_length_session = event_count_session.max()

            print("\033[1mNumber of events per user path/session\033[0m")
            out_df = pd.DataFrame(
                [
                    ["mean events", mean_user, mean_session],
                    ["std events", std_user, std_session],
                    ["median events", median_user, median_session],
                    ["min events", min_length_user, min_length_session],
                    ["max events", max_length_user, max_length_session],
                ],
                columns=["metric", "per user path", "per session"],
            ).set_index("metric")
        else:
            print("\033[1mNumber of events per user path\033[0m")
            out_df = pd.DataFrame(
                [
                    ["mean events", mean_user],
                    ["std events", std_user],
                    ["median events", median_user],
                    ["min events", min_length_user],
                    ["max events", max_length_user],
                ],
                columns=["metric", "per user path"],
            ).set_index("metric")
        display(out_df)

    def describe_events(
        self, session_col: Optional[str] = "session_id", event_list: Optional[List[str] | str] = "all"
    ) -> None:
        """
        Displays general information on the eventstream events. If session_col is present in eventstream columns, also
        outputs session statistics, assuming session_col is the session identifier column.
        """
        user_col, event_col, time_col, type_col = (
            self.schema.user_id,
            self.schema.event_name,
            self.schema.event_timestamp,
            self.schema.event_type,
        )

        df = self.to_dataframe()

        if event_list != "all":
            if type(event_list) is not list:
                raise TypeError('event_list should either be "all", or a list of event names to include.')
            df = df[df[event_col].isin(event_list)]

        if session_col in df.columns:
            has_sessions = True
        else:
            has_sessions = False

        df["__event_trajectory_idx"] = df.groupby(user_col).cumcount()
        df["__event_trajectory_timedelta"] = df[time_col] - df.groupby(user_col)[time_col].transform("first")
        total_events = df.shape[0]
        unique_users = df[user_col].nunique()

        unique_sessions = None
        if has_sessions:
            df["__event_session_idx"] = df.groupby(session_col).cumcount()
            df["__event_session_timedelta"] = df[time_col] - df.groupby(session_col)[time_col].transform("first")
            unique_sessions = df[session_col].nunique()  # type: ignore

        for i, event_name in enumerate(df[event_col].unique()):
            if i != 0:
                print("============================================================================")
                print()

            event_data = df[df[event_col] == event_name]

            print(f'\033[1m"{event_name}" event statistics:\033[0m')
            print()

            print("----------------------------------------------------------------------------")
            print("\033[1mBasic statistics\033[0m")
            print()

            event_share = round(event_data.shape[0] / total_events, 4)
            unique_users_event = event_data[user_col].nunique()
            user_event_share = round(unique_users_event / unique_users, 4)

            if has_sessions:
                unique_sessions_event = event_data[session_col].nunique()  # type: ignore
                session_event_share = round(unique_sessions_event / unique_sessions, 4)
                out_df = pd.DataFrame(
                    [
                        ["first appearance", event_data[time_col].min()],
                        ["last appearance", event_data[time_col].max()],
                        ["number of occurrences", event_data.shape[0]],
                        ["share of all events", str(event_share * 100) + "%"],
                        ["users with the event", unique_users_event],
                        ["share of users with the event", str(user_event_share * 100) + "%"],
                        ["sessions with the event", unique_sessions_event],
                        ["share of sessions with the event", str(session_event_share * 100) + "%"],
                    ],
                    columns=["metric", "value"],
                ).set_index("metric")
            else:
                out_df = pd.DataFrame(
                    [
                        ["first appearance", event_data[time_col].min()],
                        ["last appearance", event_data[time_col].max()],
                        ["number of occurrences", event_data.shape[0]],
                        ["share of all events", str(event_share * 100) + "%"],
                        ["users with the event", unique_users_event],
                        ["share of users with the event", str(user_event_share * 100) + "%"],
                    ],
                    columns=["metric", "value"],
                ).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)[event_col].agg("count")
            mean_events_user, std_events_user, median_events_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_events_user, max_events_user = user_agg.min(), user_agg.max()
            print("----------------------------------------------------------------------------")
            if has_sessions:
                print("\033[1mAppearances per user path/session\033[0m")
                print()
                session_agg = event_data.groupby(session_col)[event_col].agg("count")  # type: ignore
                mean_events_session, std_events_session, median_events_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_events_session, max_events_session = session_agg.min(), session_agg.max()
                out_df = pd.DataFrame(
                    [
                        ["mean appearances", mean_events_user, mean_events_session],
                        ["std appearances", std_events_user, std_events_session],
                        ["median appearances", median_events_user, median_events_session],
                        ["min appearances", min_events_user, min_events_session],
                        ["max appearances", max_events_user, max_events_session],
                    ],
                    columns=["metric", "per user path", "per session"],
                ).set_index("metric")
            else:
                print("\033[1mAppearances per user path\033[0m")
                print()
                out_df = pd.DataFrame(
                    [
                        ["mean appearances", mean_events_user],
                        ["std appearances", std_events_user],
                        ["median appearances", median_events_user],
                        ["min appearances", min_events_user],
                        ["max appearances", max_events_user],
                    ],
                    columns=["metric", "per user path"],
                ).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)["__event_trajectory_timedelta"].min()
            mean_time_user, std_time_user, median_time_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_time_user, max_time_user = user_agg.min(), user_agg.max()
            print("----------------------------------------------------------------------------")
            if has_sessions:
                print("\033[1mTime before first appearance since user path/session start\033[0m")
                print()
                session_agg = event_data.groupby(session_col)["__event_session_timedelta"].min()  # type: ignore
                mean_time_session, std_time_session, median_time_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_time_session, max_time_session = session_agg.min(), session_agg.max()
                out_df = pd.DataFrame(
                    [
                        ["mean timedelta", mean_time_user, mean_time_session],
                        ["std timedelta", std_time_user, std_time_session],
                        ["median timedelta", median_time_user, median_time_session],
                        ["min timedelta", min_time_user, min_time_session],
                        ["max timedelta", max_time_user, max_time_session],
                    ],
                    columns=["metric", "per user path", "per session"],
                ).set_index("metric")
            else:
                print("\033[1mTime before first appearance since user path start\033[0m")
                print()
                out_df = pd.DataFrame(
                    [
                        ["mean timedelta", mean_time_user],
                        ["std timedelta", std_time_user],
                        ["median timedelta", median_time_user],
                        ["min timedelta", min_time_user],
                        ["max timedelta", max_time_user],
                    ],
                    columns=["metric", "per user path"],
                ).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)["__event_trajectory_idx"].min()
            mean_events_user, std_events_user, median_events_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_events_user, max_events_user = user_agg.min(), user_agg.max()
            print("----------------------------------------------------------------------------")
            if has_sessions:
                print("\033[1mEvents before first appearance since user path/session start\033[0m")
                print()
                session_agg = event_data.groupby(user_col)["__event_session_idx"].min()
                mean_events_session, std_events_session, median_events_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_events_session, max_events_session = session_agg.min(), session_agg.max()
                out_df = pd.DataFrame(
                    [
                        ["mean events", mean_events_user, mean_events_session],
                        ["std events", std_events_user, std_events_session],
                        ["median events", median_events_user, median_events_session],
                        ["min events", min_events_user, min_events_session],
                        ["max events", max_events_user, max_events_session],
                    ],
                    columns=["metric", "per user path", "per session"],
                ).set_index("metric")
            else:
                print("\033[1mEvents before first appearance since user path start\033[0m")
                print()
                out_df = pd.DataFrame(
                    [
                        ["mean events", mean_events_user],
                        ["std events", std_events_user],
                        ["median events", median_events_user],
                        ["min events", min_events_user],
                        ["max events", max_events_user],
                    ],
                    columns=["metric", "per user path"],
                ).set_index("metric")
            display(out_df)

    def transition_graph(
        self,
        thresholds: dict[str, Threshold] | None = None,
        norm_type: NormType = None,
        weights: dict[str, str] | None = None,
        targets: dict[str, str | None] | None = None,
        width: int = 960,
        height: int = 900,
    ) -> TransitionGraph:
        self.__transition_graph = TransitionGraph(
            eventstream=self,
            graph_settings={},  # type: ignore
            norm_type=norm_type,
            weights=weights,
            thresholds=thresholds,
            targets=targets,
        )
        self.__transition_graph.plot_graph(
            thresholds=thresholds, targets=targets, weights=weights, width=width, height=height, norm_type=norm_type
        )
        return self.__transition_graph

    def processing_graph(self) -> PGraph:
        if self.__p_graph is None:
            self.__p_graph = PGraph(source_stream=self)
        self.__p_graph.display()
        return self.__p_graph

    def transition_adjacency(self, weights: list[str] | None = None, norm_type: NormType = None) -> pd.DataFrame:
        if self.__transition_graph is None:
            self.__transition_graph = TransitionGraph(
                eventstream=self,
                graph_settings={},  # type: ignore
                norm_type=norm_type,
            )
        adjacency = self.__transition_graph.get_adjacency(weights=weights, norm_type=norm_type)
        return adjacency
