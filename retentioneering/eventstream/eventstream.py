# flake8: noqa
from __future__ import annotations

import uuid
import warnings
from collections.abc import Collection
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Literal, MutableMapping, Optional, Tuple

import numpy as np
import pandas as pd

from retentioneering.backend import counter
from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
    tracker,
)
from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from retentioneering.eventstream.types import (
    EventstreamType,
    RawDataCustomColSchema,
    RawDataSchemaType,
)
from retentioneering.preprocessing_graph import PreprocessingGraph
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
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.utils.list import find_index

from .helpers import (
    AddNegativeEventsHelperMixin,
    AddPositiveEventsHelperMixin,
    AddStartEndEventsHelperMixin,
    CollapseLoopsHelperMixin,
    DropPathsHelperMixin,
    FilterEventsHelperMixin,
    GroupEventsBulkHelperMixin,
    GroupEventsHelperMixin,
    LabelCroppedPathsHelperMixin,
    LabelLostUsersHelperMixin,
    LabelNewUsersHelperMixin,
    PipeHelperMixin,
    RenameHelperMixin,
    SplitSessionsHelperMixin,
    TruncatePathsHelperMixin,
)
from .utils import eventstream_index

IndexOrder = List[Optional[str]]
EventsOrder = List[str]
FeatureType = Literal["tfidf", "count", "frequency", "binary", "time", "time_fraction", "external"]
NgramRange = Tuple[int, int]
Method = Literal["kmeans", "gmm"]

DEFAULT_INDEX_ORDER: IndexOrder = [
    "profile",
    "path_start",
    "new_user",
    "existing_user",
    "cropped_left",
    "session_start",
    "session_start_cropped",
    "group_alias",
    "raw",
    "raw_sleep",
    None,
    "synthetic",
    "synthetic_sleep",
    "positive_target",
    "negative_target",
    "session_end_cropped",
    "session_end",
    "session_sleep",
    "cropped_right",
    "absent_user",
    "lost_user",
    "path_end",
]


# @TODO: проработать резервирование колонок


class Eventstream(
    CollapseLoopsHelperMixin,
    DropPathsHelperMixin,
    FilterEventsHelperMixin,
    GroupEventsHelperMixin,
    GroupEventsBulkHelperMixin,
    LabelLostUsersHelperMixin,
    AddNegativeEventsHelperMixin,
    LabelNewUsersHelperMixin,
    AddPositiveEventsHelperMixin,
    SplitSessionsHelperMixin,
    AddStartEndEventsHelperMixin,
    LabelCroppedPathsHelperMixin,
    TruncatePathsHelperMixin,
    RenameHelperMixin,
    PipeHelperMixin,
    EventstreamType,
):
    """
    Collection of tools for storing and processing clickstream data.

    Parameters
    ----------
    raw_data : pd.DataFrame or pd.Series
        Raw clickstream data.
    raw_data_schema : dict or RawDataSchema, optional
        Represents mapping rules connecting important eventstream columns with the raw data columns.
        The keys are defined in :py:class:`.RawDataSchema`. The values are the corresponding column names
        in the raw data. ``custom_cols`` key stands for the defining additional columns that can be used in
        the eventstream. See the :ref:`Eventstream user guide <eventstream_raw_data_schema>` for the details.

    schema : dict or EventstreamSchema, optional
        Represents a schema of the created eventstream. The keys are defined in
        :py:class:`.EventstreamSchema`. The values are the names of the corresponding eventstream columns.
        See the :ref:`Eventstream user guide <eventstream_field_names>` for the details.

    custom_cols : list of str, optional
        The list of additional columns from the raw data to be included in the eventstream.
        If not defined, all the columns from the raw data are included.

    prepare : bool, default True
        - If ``True``, input data will be transformed in the following way:

            * ``event_timestamp`` column is converted to pandas datetime format.
            * | ``event_type`` column is added and filled with ``raw`` value.
              | If the column exists, it remains unchanged.

        - If ``False`` - ``raw_data`` will be remained as is.

    index_order : list of str, default DEFAULT_INDEX_ORDER
        Sorting order for ``event_type`` column.
    user_sample_size : int of float, optional
        Number (``int``) or share (``float``) of all users' trajectories that will be randomly chosen
        and left in final sample (all other trajectories will be removed) .
        See :numpy_random_choice:`numpy documentation<>`.
    user_sample_seed : int, optional
        A seed value that is used to generate user samples.
        See :numpy_random_seed:`numpy documentation<>`.
    events_order : list of str, optional
        Sorting order for ``event_name`` column, if there are events with equal timestamps inside each user trajectory.
        The order of raw events is fixed once while eventstream initialization.
    add_start_end_events : bool, default True
        If True, ``path_start`` and ``path_end`` synthetic events are added to each path explicitly.
        See also :py:class:`.AddStartEndEvents` documentation.

    Notes
    -----
    See :doc:`Eventstream user guide</user_guides/eventstream>` for the details.


    """

    schema: EventstreamSchema
    events_order: EventsOrder
    index_order: IndexOrder
    __hash: str = ""
    _preprocessing_graph: PreprocessingGraph | None = None
    __clusters: Clusters | None = None

    __raw_data_schema: RawDataSchemaType
    __events: pd.DataFrame | pd.Series[Any]
    __funnel: Funnel
    __cohorts: Cohorts
    __step_matrix: StepMatrix
    __sankey: StepSankey
    __stattests: StatTests
    __transition_graph: TransitionGraph
    __timedelta_hist: TimedeltaHist
    __user_lifetime_hist: UserLifetimeHist
    __event_timestamp_hist: EventTimestampHist
    __eventstream_index: int

    @time_performance(
        scope="eventstream",
        event_name="init",
    )
    def __init__(
        self,
        raw_data: pd.DataFrame | pd.Series[Any],
        raw_data_schema: RawDataSchema
        | RawDataSchemaType
        | dict[str, str | list[RawDataCustomColSchema]]
        | None = None,
        schema: EventstreamSchema | dict[str, str | list[str]] | None = None,
        prepare: bool = True,
        index_order: Optional[IndexOrder] = None,
        user_sample_size: Optional[int | float] = None,
        user_sample_seed: Optional[int] = None,
        events_order: Optional[EventsOrder] = None,
        custom_cols: List[str] | None = None,
        add_start_end_events: bool = True,
    ) -> None:
        tracking_params = dict(
            raw_data=raw_data,
            prepare=prepare,
            index_order=index_order,
            user_sample_size=user_sample_size,
            user_sample_seed=user_sample_seed,
            events_order=events_order,
            custom_cols=custom_cols,
            add_start_end_events=add_start_end_events,
        )
        not_hash_values = ["raw_data_schema", "schema"]

        if not schema:
            schema = EventstreamSchema()
        elif isinstance(schema, dict):
            schema = EventstreamSchema(**schema)  # type: ignore

        self.schema = schema
        self.__eventstream_index: int = counter.get_eventstream_index()

        if not raw_data_schema:
            raw_data_schema = RawDataSchema()
            if self.schema.event_type in raw_data.columns:
                raw_data_schema.event_type = self.schema.event_type

        elif isinstance(raw_data_schema, dict):
            raw_data_schema = RawDataSchema(**raw_data_schema)  # type: ignore
        self.__raw_data_schema = raw_data_schema

        if custom_cols is None and not self.__raw_data_schema.custom_cols and not self.schema.custom_cols:
            custom_cols = self.__define_default_custom_cols(raw_data=raw_data)

        if custom_cols and prepare:
            self.__raw_data_schema.custom_cols = []
            self.schema.custom_cols = []
            for col_name in custom_cols:
                col: RawDataCustomColSchema = {"raw_data_col": col_name, "custom_col": col_name}
                self.__raw_data_schema.custom_cols.append(col)
                self.schema.custom_cols.append(col_name)

        raw_data_schema_default_values = asdict(RawDataSchema())
        schema_default_values = asdict(EventstreamSchema())
        if isinstance(raw_data_schema, RawDataSchema):
            tracking_params["raw_data_schema"] = [
                key for key, value in raw_data_schema_default_values.items() if asdict(raw_data_schema)[key] != value
            ]
        tracking_params["schema"] = [
            key for key, value in schema_default_values.items() if asdict(self.schema)[key] != value
        ]

        self.__track_dataset(
            name="metadata",
            data=raw_data,
            params=tracking_params,
            schema=self.__raw_data_schema,
            not_hash_values=not_hash_values,
        )

        if user_sample_size is not None:
            raw_data = self.__sample_user_paths(raw_data, raw_data_schema, user_sample_size, user_sample_seed)
        if not index_order:
            self.index_order = DEFAULT_INDEX_ORDER
        else:
            self.index_order = index_order

        if events_order is not None:
            self.events_order = events_order
        else:
            self.events_order = []

        self.__events = self.__prepare_events(raw_data) if prepare else raw_data
        self.__events = self.__required_cleanup(events=self.__events)
        self.__apply_default_dataprocessors(add_start_end_events=add_start_end_events)
        self.index_events()
        if prepare:
            self.__track_dataset(
                name="metadata",
                data=self.__events,
                params=tracking_params,
                schema=self.schema,
                not_hash_values=not_hash_values,
            )

        self._preprocessing_graph = None

    @property
    def _eventstream_index(self) -> int:
        return self.__eventstream_index

    @property
    def _hash(self) -> str:
        if self.__hash == "":
            self.__hash = hash_dataframe(self.__events)
        return self.__hash

    def __track_dataset(
        self,
        name: str,
        data: pd.DataFrame | pd.Series[Any],
        params: dict[str, Any],
        schema: RawDataSchema | RawDataSchemaType | EventstreamSchema,
        not_hash_values: list[str],
    ) -> None:
        try:
            unique_users = data[schema.user_id].nunique()
        except Exception as e:
            unique_users = None

        try:
            unique_events = data[schema.event_name].nunique()
        except Exception as e:
            unique_events = None
        try:
            hist_data = data[schema.user_id].drop_duplicates()
            if len(hist_data) >= 500:
                hist_data = hist_data.sample(500, random_state=42)
            eventstream_hist = (
                data[data[schema.user_id].isin(hist_data)].groupby(schema.user_id).size().value_counts().to_dict()
            )

        except Exception:
            eventstream_hist = {}
        eventstream_hash = hash_dataframe(data=data)
        self.__hash = eventstream_hash

        performance_data: dict[str, Any] = {
            "shape": data.shape,
            "custom_cols": len(self.schema.custom_cols),
            "unique_users": unique_users,
            "unique_events": unique_events,
            "hash": self._hash,
            "eventstream_hist": eventstream_hist,
            "index": self.__eventstream_index,
        }
        collect_data_performance(
            scope="eventstream",
            event_name=name,
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data=performance_data,
            eventstream_index=self._eventstream_index,
        )

    @time_performance(
        scope="eventstream",
        event_name="copy",
    )
    def copy(self) -> Eventstream:
        """
        Make a copy of current ``eventstream``.

        Returns
        -------
        Eventstream

        """
        copied_eventstream = Eventstream(
            raw_data_schema=self.__raw_data_schema.copy(),
            raw_data=self.__events.copy(),
            schema=self.schema.copy(),
            prepare=False,
            index_order=self.index_order.copy(),
            events_order=self.events_order.copy(),
            add_start_end_events=False,
        )
        collect_data_performance(
            scope="eventstream",
            event_name="metadata",
            called_params={},
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=copied_eventstream._eventstream_index,
        )
        return copied_eventstream

    @time_performance(
        scope="eventstream",
        event_name="append_eventstream",
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

        collect_data_performance(
            scope="eventstream",
            event_name="metadata",
            called_params={},
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=eventstream._eventstream_index,
            child_eventstream_index=self._eventstream_index,
        )

        curr_events = self.to_dataframe()
        new_events = eventstream.to_dataframe()

        merged_events = pd.merge(
            curr_events,
            new_events,
            left_on=self.schema.event_id,
            right_on=self.schema.event_id,
            how="outer",
            indicator=True,
        )

        left_events = merged_events[(merged_events["_merge"] == "left_only")]
        both_events = merged_events[(merged_events["_merge"] == "both")]
        right_events = merged_events[(merged_events["_merge"] == "right_only")]

        right_events = pd.concat([right_events, both_events])

        cols = self.schema.get_cols()

        result_left_part = pd.DataFrame()
        result_right_part = pd.DataFrame()
        result_both_part = pd.DataFrame()

        with warnings.catch_warnings():
            # disable warning for pydantic schema Callable type
            warnings.simplefilter(action="ignore", category=FutureWarning)

            for col in cols:
                result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
                result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        self.__events = pd.concat([result_left_part, result_both_part, result_right_part])
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

    def _create_index(self, events: pd.DataFrame) -> pd.DataFrame:
        events_order_sort_col = "events_order_sort_col"
        events_type_sort_col = "events_type_sort_col"

        events[events_order_sort_col] = events[self.schema.event_name].apply(
            lambda e: self.__get_events_priority_by_config(e)
        )
        events[events_type_sort_col] = events[self.schema.event_type].apply(
            lambda e: self.__get_events_priority_by_type(e)
        )

        events = events.sort_values([self.schema.event_timestamp, events_order_sort_col, events_type_sort_col])  # type: ignore
        events = events.drop([events_order_sort_col, events_type_sort_col], axis=1)
        events.reset_index(inplace=True, drop=True)
        events[self.schema.event_index] = events.index
        return events

    @time_performance(
        scope="eventstream",
        event_name="to_dataframe",
    )
    def to_dataframe(self, copy: bool = False) -> pd.DataFrame:
        """
        Convert ``eventstream`` to ``pd.Dataframe``

        Parameters
        ----------
        copy : bool, default False
            If ``True`` - copy data from current ``eventstream``.
            See details in the :pandas_copy:`pandas documentation<>`.

        Returns
        -------
        pd.DataFrame

        """
        params: dict[str, Any] = {
            "copy": copy,
        }

        events = self.__events
        view = pd.DataFrame(events, columns=self.schema.get_cols(), copy=copy)
        self.__track_dataset(name="metadata", data=view, params=params, schema=self.schema, not_hash_values=[])
        return view

    @time_performance(
        scope="eventstream",
        event_name="index_events",
    )
    def index_events(self) -> None:
        """
        Sort and index eventstream using DEFAULT_INDEX_ORDER.

        Returns
        -------
        None

        """
        collect_data_performance(
            scope="eventstream",
            event_name="metadata",
            called_params={},
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        order_temp_col_name = "order"
        indexed = self.__events

        indexed[order_temp_col_name] = indexed[self.schema.event_type].apply(
            lambda e: self.__get_events_priority_by_type(e)
        )
        indexed = indexed.sort_values([self.schema.event_timestamp, self.schema.event_index, order_temp_col_name])  # type: ignore
        indexed = indexed.drop([order_temp_col_name], axis=1)
        indexed.reset_index(inplace=True, drop=True)
        self.__events = indexed

    @time_performance(
        scope="eventstream",
        event_name="add_custom_col",
    )
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
        collect_data_performance(
            scope="eventstream",
            event_name="metadata",
            called_params={"name": name, "data": data},
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__raw_data_schema.custom_cols.extend([{"custom_col": name, "raw_data_col": name}])
        self.schema.custom_cols.extend([name])
        self.__events[name] = data

    def __define_default_custom_cols(self, raw_data: pd.DataFrame | pd.Series[Any]) -> List[str]:
        raw_data_cols = self.__raw_data_schema.get_default_cols()
        schema_cols = self.schema.get_default_cols()

        cols_denylist: List[str] = raw_data_cols + schema_cols

        custom_cols: List[str] = []

        for raw_col_name in raw_data.columns:
            if raw_col_name in cols_denylist:
                continue
            custom_cols.append(raw_col_name)

        return custom_cols

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

        # clear df
        events.drop(list(events.columns), axis=1, inplace=True)

        if self.__raw_data_schema.event_id is not None and self.__raw_data_schema.event_id in raw_data.columns:
            events[self.schema.event_id] = raw_data[self.__raw_data_schema.event_id]
        else:
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

        if self.__raw_data_schema.event_index is not None and self.__raw_data_schema.event_index in raw_data.columns:
            events[self.schema.event_index] = raw_data[self.__raw_data_schema.event_index].astype("int64")
        else:
            events = self._create_index(events=events)  # type: ignore

        return events

    def __apply_default_dataprocessors(self, add_start_end_events: bool) -> None:
        from retentioneering.data_processors_lib.add_start_end_events import (
            AddStartEndEvents,
            AddStartEndEventsParams,
        )

        events = self.__events
        name_col = self.schema.event_name

        if add_start_end_events:
            add_start_end_processor = AddStartEndEvents(AddStartEndEventsParams())
            self.__events = add_start_end_processor.apply(self.__events, self.schema)  # type: ignore

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

    def __get_events_priority_by_type(self, event_type: Optional[str]) -> int:
        if event_type in self.index_order:
            return self.index_order.index(event_type)
        return len(self.index_order)

    def __get_events_priority_by_config(self, event_name: str) -> int:
        if event_name in self.events_order:
            return self.events_order.index(event_name)
        return len(self.events_order)

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

    @time_performance(
        scope="funnel",
        event_name="helper",
        event_value="plot",
    )
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
        params = {
            "stages": stages,
            "stage_names": stage_names,
            "funnel_type": funnel_type,
            "segments": segments,
            "segment_names": segment_names,
            "show_plot": show_plot,
        }

        collect_data_performance(
            scope="funnel",
            event_name="metadata",
            called_params=params,
            not_hash_values=["funnel_type"],
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        self.__funnel = Funnel(eventstream=self)
        self.__funnel.fit(
            stages=stages,
            stage_names=stage_names,
            funnel_type=funnel_type,
            segments=segments,
            segment_names=segment_names,
        )
        if show_plot:
            figure = self.__funnel.plot()
            figure.show()
        return self.__funnel

    @property
    @time_performance(
        scope="clusters",
        event_name="helper",
        event_value="init",
    )
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

    @time_performance(
        scope="step_matrix",
        event_name="helper",
        event_value="plot",
    )
    def step_matrix(
        self,
        max_steps: int = 20,
        weight_col: str | None = None,
        precision: int = 2,
        targets: list[str] | str | None = None,
        accumulated: Literal["both", "only"] | None = None,
        sorting: list | None = None,
        threshold: float = 0.01,
        centered: dict | None = None,
        groups: Tuple[list, list] | None = None,
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

        params = {
            "max_steps": max_steps,
            "weight_col": weight_col,
            "precision": precision,
            "targets": targets,
            "accumulated": accumulated,
            "sorting": sorting,
            "threshold": threshold,
            "centered": centered,
            "groups": groups,
            "show_plot": show_plot,
        }
        not_hash_values = ["accumulated", "centered"]
        collect_data_performance(
            scope="step_matrix",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        self.__step_matrix = StepMatrix(eventstream=self)

        self.__step_matrix.fit(
            max_steps=max_steps,
            weight_col=weight_col,
            precision=precision,
            targets=targets,
            accumulated=accumulated,
            sorting=sorting,
            threshold=threshold,
            centered=centered,
            groups=groups,
        )
        if show_plot:
            self.__step_matrix.plot()
        return self.__step_matrix

    @time_performance(
        scope="step_sankey",
        event_name="helper",
        event_value="plot",
    )
    def step_sankey(
        self,
        max_steps: int = 10,
        threshold: int | float = 0.05,
        sorting: list | None = None,
        targets: list[str] | str | None = None,
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

        params = {
            "max_steps": max_steps,
            "threshold": threshold,
            "sorting": sorting,
            "targets": targets,
            "autosize": autosize,
            "width": width,
            "height": height,
            "show_plot": show_plot,
        }

        collect_data_performance(
            scope="step_sankey",
            event_name="metadata",
            called_params=params,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__sankey = StepSankey(eventstream=self)

        self.__sankey.fit(max_steps=max_steps, threshold=threshold, sorting=sorting, targets=targets)
        if show_plot:
            figure = self.__sankey.plot(autosize=autosize, width=width, height=height)
            figure.show()
        return self.__sankey

    @time_performance(
        scope="cohorts",
        event_name="helper",
        event_value="heatmap",
    )
    def cohorts(
        self,
        cohort_start_unit: DATETIME_UNITS,
        cohort_period: Tuple[int, DATETIME_UNITS],
        average: bool = True,
        cut_bottom: int = 0,
        cut_right: int = 0,
        cut_diagonal: int = 0,
        width: float = 5.0,
        height: float = 5.0,
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

        params = {
            "cohort_start_unit": cohort_start_unit,
            "cohort_period": cohort_period,
            "average": average,
            "cut_bottom": cut_bottom,
            "cut_right": cut_right,
            "cut_diagonal": cut_diagonal,
            "width": width,
            "height": height,
            "show_plot": show_plot,
        }

        not_hash_values = ["cohort_start_unit", "cohort_period"]
        collect_data_performance(
            scope="cohorts",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__cohorts = Cohorts(eventstream=self)

        self.__cohorts.fit(
            cohort_start_unit=cohort_start_unit,
            cohort_period=cohort_period,
            average=average,
            cut_bottom=cut_bottom,
            cut_right=cut_right,
            cut_diagonal=cut_diagonal,
        )
        if show_plot:
            self.__cohorts.heatmap(width=width, height=height)
        return self.__cohorts

    @time_performance(
        scope="stattests",
        event_name="helper",
        event_value="display_results",
    )
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
        params = {
            "test": test,
            "groups": groups,
            "func": func,
            "group_names": group_names,
            "alpha": alpha,
        }
        not_hash_values = ["test"]

        collect_data_performance(
            scope="stattests",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__stattests = StatTests(eventstream=self)
        self.__stattests.fit(groups=groups, func=func, test=test, group_names=group_names, alpha=alpha)
        self.__stattests.display_results()
        return self.__stattests

    @time_performance(
        scope="timedelta_hist",
        event_name="helper",
        event_value="plot",
    )
    def timedelta_hist(
        self,
        raw_events_only: bool = False,
        event_pair: list[str | Literal[EVENTSTREAM_GLOBAL_EVENTS]] | None = None,
        adjacent_events_only: bool = True,
        weight_col: str | None = None,
        time_agg: AGGREGATION_NAMES | None = None,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        width: float = 6.0,
        height: float = 4.5,
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

        params = {
            "raw_events_only": raw_events_only,
            "event_pair": event_pair,
            "adjacent_events_only": adjacent_events_only,
            "weight_col": weight_col,
            "time_agg": time_agg,
            "timedelta_unit": timedelta_unit,
            "log_scale": log_scale,
            "lower_cutoff_quantile": lower_cutoff_quantile,
            "upper_cutoff_quantile": upper_cutoff_quantile,
            "bins": bins,
            "width": width,
            "height": height,
            "show_plot": show_plot,
        }
        not_hash_values = ["time_agg", "timedelta_unit"]

        collect_data_performance(
            scope="timedelta_hist",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__timedelta_hist = TimedeltaHist(
            eventstream=self,
        )

        self.__timedelta_hist.fit(
            raw_events_only=raw_events_only,
            event_pair=event_pair,
            adjacent_events_only=adjacent_events_only,
            time_agg=time_agg,
            weight_col=weight_col,
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
        )
        if show_plot:
            self.__timedelta_hist.plot(
                width=width,
                height=height,
            )

        return self.__timedelta_hist

    @time_performance(
        scope="user_lifetime_hist",
        event_name="helper",
        event_value="plot",
    )
    def user_lifetime_hist(
        self,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        width: float = 6.0,
        height: float = 4.5,
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
        params = {
            "timedelta_unit": timedelta_unit,
            "log_scale": log_scale,
            "lower_cutoff_quantile": lower_cutoff_quantile,
            "upper_cutoff_quantile": upper_cutoff_quantile,
            "bins": bins,
            "width": width,
            "height": height,
            "show_plot": show_plot,
        }
        not_hash_values = ["timedelta_unit"]

        collect_data_performance(
            scope="user_lifetime_hist",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        self.__user_lifetime_hist = UserLifetimeHist(
            eventstream=self,
        )
        self.__user_lifetime_hist.fit(
            timedelta_unit=timedelta_unit,
            log_scale=log_scale,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
        )
        if show_plot:
            self.__user_lifetime_hist.plot(width=width, height=height)
        return self.__user_lifetime_hist

    @time_performance(
        scope="event_timestamp_hist",
        event_name="helper",
        event_value="plot",
    )
    def event_timestamp_hist(
        self,
        event_list: list[str] | None = None,
        raw_events_only: bool = False,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        width: float = 6.0,
        height: float = 4.5,
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
        params = {
            "event_list": event_list,
            "raw_events_only": raw_events_only,
            "lower_cutoff_quantile": lower_cutoff_quantile,
            "upper_cutoff_quantile": upper_cutoff_quantile,
            "bins": bins,
            "width": width,
            "height": height,
            "show_plot": show_plot,
        }

        collect_data_performance(
            scope="event_timestamp_hist",
            event_name="metadata",
            called_params=params,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        self.__event_timestamp_hist = EventTimestampHist(
            eventstream=self,
        )

        self.__event_timestamp_hist.fit(
            event_list=event_list,
            raw_events_only=raw_events_only,
            lower_cutoff_quantile=lower_cutoff_quantile,
            upper_cutoff_quantile=upper_cutoff_quantile,
            bins=bins,
        )
        if show_plot:
            self.__event_timestamp_hist.plot(width=width, height=height)
        return self.__event_timestamp_hist

    @time_performance(
        scope="describe",
        event_name="helper",
        event_value="_values",
    )
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
        params = {
            "session_col": session_col,
            "raw_events_only": raw_events_only,
        }

        collect_data_performance(
            scope="describe",
            event_name="metadata",
            called_params=params,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )
        describer = _Describe(eventstream=self, session_col=session_col, raw_events_only=raw_events_only)
        return describer._values()

    @time_performance(
        scope="describe_events",
        event_name="helper",
        event_value="_values",
    )
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

        params = {
            "session_col": session_col,
            "raw_events_only": raw_events_only,
            "event_list": event_list,
        }

        collect_data_performance(
            scope="describe_events",
            event_name="metadata",
            called_params=params,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        describer = _DescribeEvents(
            eventstream=self, session_col=session_col, event_list=event_list, raw_events_only=raw_events_only
        )
        return describer._values()

    @time_performance(
        scope="transition_graph",
        event_name="helper",
        event_value="plot",
    )
    def transition_graph(
        self,
        edges_norm_type: NormType = None,
        nodes_norm_type: NormType = None,
        targets: MutableMapping[str, str | None] | None = None,
        nodes_threshold: Threshold | None = None,
        edges_threshold: Threshold | None = None,
        nodes_weight_col: str | None = None,
        edges_weight_col: str | None = None,
        custom_weight_cols: list[str] | None = None,
        width: str | int | float = "100%",
        height: str | int | float = "60vh",
        show_weights: bool = True,
        show_percents: bool = False,
        show_nodes_names: bool = True,
        show_all_edges_for_targets: bool = True,
        show_nodes_without_links: bool = False,
        show_edge_info_on_hover: bool = True,
        layout_dump: str | None = None,
        nodes_custom_colors: Dict[str, str] | None = None,
        edges_custom_colors: Dict[Tuple[str, str], str] | None = None,
    ) -> TransitionGraph:
        """

        Parameters
        ----------
        See parameters' description
            :py:meth:`.TransitionGraph.plot`
            @TODO: maybe load docs with docrep? 2dpanina, Vladimir Makhanov

        Returns
        -------
        TransitionGraph
            Rendered IFrame graph.

        """

        params = {
            "edges_norm_type": edges_norm_type,
            "nodes_norm_type": nodes_norm_type,
            "targets": targets,
            "nodes_threshold": nodes_threshold,
            "edges_threshold": edges_threshold,
            "nodes_weight_col": nodes_weight_col,
            "edges_weight_col": edges_weight_col,
            "custom_weight_cols": custom_weight_cols,
            "width": width,
            "height": height,
            "show_weights": show_weights,
            "show_percents": show_percents,
            "show_nodes_names": show_nodes_names,
            "show_all_edges_for_targets": show_all_edges_for_targets,
            "show_nodes_without_links": show_nodes_without_links,
            "show_edge_info_on_hover": show_edge_info_on_hover,
            "layout_dump": layout_dump,
        }
        not_hash_values = ["edges_norm_type", "targets", "width", "height"]

        collect_data_performance(
            scope="transition_graph",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        self.__transition_graph = TransitionGraph(eventstream=self)
        self.__transition_graph.plot(
            targets=targets,
            edges_norm_type=edges_norm_type,
            nodes_norm_type=nodes_norm_type,
            edges_weight_col=edges_weight_col,
            nodes_threshold=nodes_threshold,
            edges_threshold=edges_threshold,
            nodes_weight_col=nodes_weight_col,
            custom_weight_cols=custom_weight_cols,
            width=width,
            height=height,
            show_weights=show_weights,
            show_percents=show_percents,
            show_nodes_names=show_nodes_names,
            show_all_edges_for_targets=show_all_edges_for_targets,
            show_nodes_without_links=show_nodes_without_links,
            layout_dump=layout_dump,
            show_edge_info_on_hover=show_edge_info_on_hover,
            nodes_custom_colors=nodes_custom_colors,
            edges_custom_colors=edges_custom_colors,
        )
        return self.__transition_graph

    @time_performance(
        scope="preprocessing_graph",
        event_name="helper",
        event_value="display",
    )
    def preprocessing_graph(self, width: int = 960, height: int = 600) -> PreprocessingGraph:
        """
        Display the preprocessing GUI tool.

        Parameters
        ----------
        width : int, default 960
            Width of plot in pixels.
        height : int, default 600
            Height of plot in pixels.

        Returns
        -------
        PreprocessingGraph
            Rendered preprocessing graph.
        """

        params = {
            "width": width,
            "height": height,
        }

        collect_data_performance(
            scope="preprocessing_graph",
            event_name="metadata",
            called_params=params,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        if self._preprocessing_graph is None:
            self._preprocessing_graph = PreprocessingGraph(source_stream=self)
        self._preprocessing_graph.display(width=width, height=height)

        return self._preprocessing_graph

    @time_performance(
        scope="transition_matrix",
        event_name="helper",
        event_value="_values",
    )
    def transition_matrix(self, weight_col: str | None = None, norm_type: NormType = None) -> pd.DataFrame:
        """
        Get transition weights as a matrix for each unique pair of events. The calculation logic is the same
        that is used for edge weights calculation of transition graph.

        Parameters
        ----------

        weight_col : str, optional
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
        not_hash_values = ["norm_type"]

        params = {"weight_col": weight_col, "norm_type": norm_type}
        collect_data_performance(
            scope="transition_matrix",
            event_name="metadata",
            called_params=params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
        )

        matrix = _TransitionMatrix(eventstream=self)
        return matrix._values(weight_col=weight_col, norm_type=norm_type)
