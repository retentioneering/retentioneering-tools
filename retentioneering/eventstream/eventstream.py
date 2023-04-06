# flake8: noqa
from __future__ import annotations

import uuid
import warnings
from collections.abc import Collection
from typing import Any, Callable, List, Literal, MutableMapping, Optional, Tuple, Union

import numpy as np
import pandas as pd

from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from retentioneering.eventstream.types import (
    EventstreamType,
    RawDataCustomColSchema,
    RawDataSchemaType,
    Relation,
)
from retentioneering.graph import PGraph
from retentioneering.tooling import (
    Clusters,
    Cohorts,
    EventTimestampHist,
    Funnel,
    StatTests,
    StepMatrix,
    StepSankey,
    TimedeltaHist,
    TransitionGraph,
    UserLifetimeHist,
)
from retentioneering.tooling._describe import _Describe
from retentioneering.tooling._describe_events import _DescribeEvents
from retentioneering.tooling._transition_matrix import _TransitionMatrix
from retentioneering.tooling.constants import BINS_ESTIMATORS
from retentioneering.tooling.stattests.constants import STATTEST_NAMES
from retentioneering.tooling.timedelta_hist.constants import (
    AGGREGATION_NAMES,
    EVENTSTREAM_GLOBAL_EVENTS,
)
from retentioneering.tooling.typing.transition_graph import NormType, Threshold
from retentioneering.utils import get_merged_col
from retentioneering.utils.list import find_index

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


# @TODO: проработать резервирование колонок


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

        - If ``raw_data`` column names are different from the default :py:class:`.RawDataSchema`.
        - If there is at least one ``custom_col`` in ``raw_data``.

    schema : EventstreamSchema, optional
        Schema of the created ``eventstream``.
        See default schema :py:class:`.EventstreamSchema`.
    prepare : bool, default True
        - If ``True``, input data will be transformed in the following way:

            * ``event_timestamp`` column is converted to pandas datetime format.
            * | ``event_type`` column is added and filled with ``raw`` value.
              | If the column exists, it remains unchanged.

        - If ``False`` - ``raw_data`` will be remained as is.

    index_order : list of str, default DEFAULT_INDEX_ORDER
        Sorting order for ``event_type`` column.
    relations : list, optional
    user_sample_size : int of float, optional
        Number (``int``) or share (``float``) of all users' trajectories that will be randomly chosen
        and left in final sample (all other trajectories will be removed) .
        See :numpy_random_choice:`numpy documentation<>`.
    user_sample_seed : int, optional
        A seed value that is used to generate user samples.
        See :numpy_random_seed:`numpy documentation<>`.

    Notes
    -----
    See :doc:`Eventstream user guide</user_guides/eventstream>` for the details.


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
    __transition_graph: TransitionGraph | None = None
    __p_graph: PGraph | None = None
    __timedelta_hist: TimedeltaHist | None = None
    __user_lifetime_hist: UserLifetimeHist | None = None
    __event_timestamp_hist: EventTimestampHist | None = None

    def __init__(
        self,
        raw_data: pd.DataFrame | pd.Series[Any],
        raw_data_schema: RawDataSchema
        | RawDataSchemaType
        | dict[str, str | list[RawDataCustomColSchema]]
        | None = None,
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
        elif isinstance(raw_data_schema, dict):
            raw_data_schema = RawDataSchema(**raw_data_schema)  # type: ignore
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
        self.__events = self.__required_cleanup(events=self.__events)
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
        user_id_type = curr_events.dtypes[self.schema.user_id]

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
        self.__events[self.schema.user_id] = self.__events[self.schema.user_id].astype(user_id_type)

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
        show_deleted : bool, default False
            If ``True`` - show all rows in ``eventstream``.
        copy : bool, default False
            If ``True`` - copy data from current ``eventstream``.
            See details in the :pandas_copy:`pandas documentation<>`.

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
        cols: list[str] | pd.Index = self.__events.columns
        raw_cols: list[str] = []
        for col in cols:
            if col.startswith(RAW_COL_PREFIX):  # type: ignore
                raw_cols.append(col)  # type: ignore
        return raw_cols

    def _get_relation_cols(self) -> list[str]:
        cols = self.__events.columns
        relation_cols: list[str] = []
        for col in cols:
            if col.startswith("ref_"):  # type: ignore
                relation_cols.append(col)  # type: ignore
        return relation_cols

    def add_custom_col(self, name: str, data: pd.Series[Any] | None) -> None:
        """
        Add custom column to an existing ``eventstream``.

        Parameters
        ----------
        name : str
            New column name.
        data : pd.Series

            - If ``pd.Series`` - new column with given values will be added.
            - If ``None`` - new column will be filled with ``np.nan``.

        Returns
        -------
        Eventstream
        """
        self.__raw_data_schema.custom_cols.extend([{"custom_col": name, "raw_data_col": name}])
        self.schema.custom_cols.extend([name])
        self.__events[name] = data

    def _soft_delete(self, events: pd.DataFrame) -> None:
        """
        Delete events either by event_id or by the last relation.
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

    def __required_cleanup(self, events: pd.DataFrame | pd.Series[Any]) -> pd.DataFrame | pd.Series[Any]:
        income_size = len(events)
        events.dropna(  # type: ignore
            subset=[self.schema.event_name, self.schema.event_timestamp, self.schema.user_id], inplace=True
        )
        size_after_cleanup = len(events)
        if (removed_rows := income_size - size_after_cleanup) > 0:
            warnings.warn(
                "Removed %s rows because they have empty %s or %s or %s"
                % (removed_rows, self.schema.event_name, self.schema.event_timestamp, self.schema.user_id)
            )
        return events

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
        funnel_type: Literal["open", "closed", "hybrid"] = "closed",
        segments: Collection[Collection[int]] | None = None,
        segment_names: list[str] | None = None,
        show_plot: bool = True,
    ) -> Funnel:
        """
        Show a visualization of the user sequential events represented as a funnel.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, a funnel visualization is shown.
        See other parameters' description
            :py:class:`.Funnel`

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
        )
        self.__funnel.fit()
        if show_plot:
            figure = self.__funnel.plot()
            figure.show()
        return self.__funnel

    @property
    def clusters(self) -> Clusters:
        """

        Returns
        -------
        Clusters
            A blank (not fitted) instance of ``Clusters`` class to be used for cluster analysis.

        See Also
        --------
        .Clusters
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
        Show a heatmap visualization of the step matrix.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, a step matrix heatmap is shown.
        See other parameters' description
            :py:class:`.StepMatrix`

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
            self.__step_matrix.plot()
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
        Show a Sankey diagram visualizing the user paths in stepwise manner.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, a sankey diagram is shown.
        See other parameters' description
            :py:class:`.StepSankey`

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
        figsize: Tuple[float, float] = (5, 5),
        show_plot: bool = True,
    ) -> Cohorts:
        """
        Show a heatmap visualization of the user appearance grouped by cohorts.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, a cohort matrix heatmap is shown.
        See other parameters' description
            :py:class:`.Cohorts`

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
        test: STATTEST_NAMES,
        groups: Tuple[list[str | int], list[str | int]],
        func: Callable,
        group_names: Tuple[str, str] = ("group_1", "group_2"),
        alpha: float = 0.05,
    ) -> StatTests:
        """
        Determine the statistical difference between the metric values in two user groups.

        Parameters
        ----------
        See parameters' description
            :py:class:`.Stattests`

        Returns
        -------
        StatTests
            A ``StatTest`` class instance fitted to the given parameters.
        """
        self.__stattests = StatTests(
            eventstream=self, groups=groups, func=func, test=test, group_names=group_names, alpha=alpha
        )
        self.__stattests.fit()
        self.__stattests.display_results()
        return self.__stattests

    def timedelta_hist(
        self,
        raw_events_only: bool = False,
        event_pair: Optional[list[str | Literal[EVENTSTREAM_GLOBAL_EVENTS]]] = None,
        only_adjacent_event_pairs: bool = True,
        weight_col: str = "user_id",
        aggregation: Optional[AGGREGATION_NAMES] = None,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        figsize: tuple[float, float] = (12.0, 7.0),
        show_plot: bool = True,
    ) -> TimedeltaHist:
        """
        Plot the distribution of the time deltas between two events. Support various
        distribution types, such as distribution of time for adjacent consecutive events, or
        for a pair of pre-defined events, or median transition time from event to event per user/session.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, histogram is shown.
        See other parameters' description
            :py:class:`.TimedeltaHist`

        Returns
        -------
        TimedeltaHist
            A ``TimedeltaHist`` class instance fitted with given parameters.

        """
        self.__timedelta_hist = TimedeltaHist(
            eventstream=self,
            raw_events_only=raw_events_only,
            event_pair=event_pair,
            only_adjacent_event_pairs=only_adjacent_event_pairs,
            aggregation=aggregation,
            weight_col=weight_col,
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
            figsize=figsize,
        )

        self.__timedelta_hist.fit()
        if show_plot:
            self.__timedelta_hist.plot()

        return self.__timedelta_hist

    def user_lifetime_hist(
        self,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        figsize: tuple[float, float] = (12.0, 7.0),
        show_plot: bool = True,
    ) -> UserLifetimeHist:
        """
        Plot the distribution of user lifetimes. A ``users lifetime`` is the timedelta between the first and the last
        events of the user.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, histogram is shown.
        See other parameters' description
            :py:class:`.UserLifetimeHist`

        Returns
        -------
        UserLifetimeHist
            A ``UserLifetimeHist`` class instance with given parameters.


        """
        self.__user_lifetime_hist = UserLifetimeHist(
            eventstream=self,
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
            figsize=figsize,
        )
        self.__user_lifetime_hist.fit()
        if show_plot:
            self.__user_lifetime_hist.plot()
        return self.__user_lifetime_hist

    def event_timestamp_hist(
        self,
        event_list: list[str] | None = None,
        raw_events_only: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        figsize: tuple[float, float] = (12.0, 7.0),
        show_plot: bool = True,
    ) -> EventTimestampHist:
        """
        Plot distribution of events over time. Can be useful for detecting time-based anomalies, and visualising
        general timespan of the eventstream.

        Parameters
        ----------
        show_plot : bool, default True
            If ``True``, histogram is shown.
        See other parameters' description
            :py:class:`.EventTimestampHist`


        Returns
        -------
        EventTimestampHist
            A ``EventTimestampHist`` class instance with given parameters.
        """
        self.__event_timestamp_hist = EventTimestampHist(
            eventstream=self,
            event_list=event_list,
            raw_events_only=raw_events_only,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
            figsize=figsize,
        )

        self.__event_timestamp_hist.fit()
        if show_plot:
            self.__event_timestamp_hist.plot()
        return self.__event_timestamp_hist

    def describe(self, session_col: str = "session_id", raw_events_only: bool = False) -> pd.DataFrame:
        """
        Display general eventstream information. If ``session_col`` is present in eventstream, also
        output session statistics.

        Parameters
        ----------
        session_col : str, default 'session_id'
            Specify name of the session column. If the column is present in the eventstream,
            session statistics will be added to the output.

        raw_events_only : bool, default False
            If ``True`` - statistics will only be shown for raw events.
            If ``False`` - statistics will be shown for all events presented in your data.

        Returns
        -------
        pd.DataFrame
            A dataframe containing descriptive statistics for the eventstream.


        See Also
        --------
        .EventTimestampHist : Plot the distribution of events over time.
        .TimedeltaHist : Plot the distribution of the time deltas between two events.
        .UserLifetimeHist : Plot the distribution of user lifetimes.
        .Eventstream.describe_events : Show general eventstream events statistics.


        Notes
        -----
        - All ``float`` values are rounded to 2.
        - All ``datetime`` values are rounded to seconds.

        See :ref:`Eventstream user guide<eventstream_describe>` for the details.


        """
        describer = _Describe(eventstream=self, session_col=session_col, raw_events_only=raw_events_only)
        return describer._values()

    def describe_events(
        self, session_col: str = "session_id", raw_events_only: bool = False, event_list: list[str] | None = None
    ) -> pd.DataFrame:
        """
        Display general information on eventstream events. If ``session_col`` is present in eventstream, also
        output session statistics.

        Parameters
        ----------
        session_col : str, default 'session_id'
            Specify name of the session column. If the column is present in the eventstream,
            output session statistics.

        raw_events_only : bool, default False
            If ``True`` - statistics will only be shown for raw events.
            If ``False`` - statistics will be shown for all events presented in your data.

        event_list : list of str, optional
            Specify events to be displayed.

        Returns
        -------
        pd.DataFrame
            **Eventstream statistics**:

            - The following metrics are calculated for each event present in the eventstream
              (or the narrowed eventstream if parameters ``event_list`` or ``raw_events_only`` are used).
              Let all_events, all_users, all_sessions be the numbers of all events, users,
              and sessions present in the eventstream. Then:

                - *number_of_occurrences* - the number of occurrences of a particular event in the eventstream;
                - *unique_users* - the number of unique users who experienced a particular event;
                - *unique_sessions* - the number of unique sessions with each event;
                - *number_of_occurrences_shared* - number_of_occurrences / all_events (raw_events_only,
                  if this parameter = ``True``);
                - *unique_users_shared* - unique_users / all_users;
                - *unique_sessions_shared* - unique_sessions / all_sessions;

            - **time_to_FO_user_wise** category - timedelta between ``path_start``
              and the first occurrence (FO) of a specified event in each user path.
            - **steps_to_FO_user_wise** category - the number of steps (events) from
              ``path_start`` to the first occurrence (FO) of a specified event in each user path.
              If ``raw_events_only=True`` only raw events will be counted.
            - **time_to_FO_session_wise** category - timedelta  between ``session_start``
              and the first occurrence (FO) of a specified event in each session.
            - **steps_to_FO_session_wise** category - the number of steps (events) from
              ``session_start`` to the first occurrence (FO) of a specified event in each session.
              If ``raw_events_only=True`` only raw events will be counted.

            Agg functions for each ``first_occurrence*`` category are: mean, std, median, min, max.

        See Also
        --------
        .EventTimestampHist : Plot the distribution of events over time.
        .TimedeltaHist : Plot the distribution of the time deltas between two events.
        .UserLifetimeHist : Plot the distribution of user lifetimes.
        .Eventstream.describe : Show general eventstream statistics.

        Notes
        -----
        - All ``float`` values are rounded to 2.
        - All ``datetime`` values are rounded to seconds.

        See :ref:`Eventstream user guide<eventstream_describe_events>` for the details.

        """
        describer = _DescribeEvents(
            eventstream=self, session_col=session_col, event_list=event_list, raw_events_only=raw_events_only
        )
        return describer._values()

    def transition_graph(
        self,
        graph_settings: dict[str, Any] | None = None,
        edges_norm_type: NormType = None,
        targets: MutableMapping[str, str | None] | None = None,
        nodes_threshold: Threshold | None = None,
        edges_threshold: Threshold | None = None,
        nodes_weight_col: str | None = None,
        edges_weight_col: str | None = None,
        custom_weight_cols: list[str] | None = None,
        width: int = 960,
        height: int = 900,
    ) -> TransitionGraph:
        """

        Parameters
        ----------
        See parameters' description
            :py:meth:`.TransitionGraph.plot`

        Returns
        -------
        TransitionGraph
            Rendered IFrame graph.

        """
        self.__transition_graph = TransitionGraph(
            eventstream=self,
            graph_settings=graph_settings,
        )
        self.__transition_graph.plot(
            targets=targets,
            edges_norm_type=edges_norm_type,
            edges_weight_col=edges_weight_col,
            nodes_threshold=nodes_threshold,
            edges_threshold=edges_threshold,
            nodes_weight_col=nodes_weight_col,
            custom_weight_cols=custom_weight_cols,
            width=width,
            height=height,
        )
        return self.__transition_graph

    def processing_graph(self) -> PGraph:
        if self.__p_graph is None:
            self.__p_graph = PGraph(source_stream=self)
        self.__p_graph.display()
        return self.__p_graph

    def transition_matrix(self, weight_col: str | None = None, norm_type: NormType = None) -> pd.DataFrame:
        """
        Get transition weights as a matrix for each unique pair of events. The calculation logic is the same
        that is used for edge weights calculation of transition graph.

        Parameters
        ----------

        weight_col : str, default None
            Weighting column for the transition weights calculation.
            See :ref:`transition graph user guide <transition_graph_weights>` for the details.

        norm_type : {"full", "node", None}, default None
            Normalization type. See :ref:`transition graph user guide <transition_graph_weights>` for the details.

        Returns
        -------
        pd.DataFrame
            Transition matrix. ``(i, j)``-th matrix value relates to the weight of i → j transition.

        Notes
        -----
        See :ref:`transition graph user guide <transition_graph_transition_matrix>` for the details.
        """

        matrix = _TransitionMatrix(eventstream=self)
        return matrix._values(weight_col=weight_col, norm_type=norm_type)
