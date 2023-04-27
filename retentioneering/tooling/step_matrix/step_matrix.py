from __future__ import annotations

import itertools
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal, Tuple

import matplotlib
import pandas as pd
import seaborn as sns

from retentioneering.backend.tracker import track
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.mixins.ended_events import EndedEventsMixin


@dataclass
class CenteredParams:
    event: str
    left_gap: int
    occurrence: int

    def __post_init__(self) -> None:
        if self.occurrence < 1:
            raise ValueError("Occurrence in 'centered' dictionary must be >=1")
        if self.left_gap < 1:
            raise ValueError("left_gap in 'centered' dictionary must be >=1")


class StepMatrix(EndedEventsMixin):
    """
    Step matrix is a matrix where its ``(i, j)`` element shows the frequency
    of event ``i`` occurring as ``j``-th step in user trajectories. This class
    provides methods for step matrix calculation and visualization.

    Parameters
    ----------
    eventstream : EventstreamType

    See Also
    --------
    .Eventstream.step_matrix : Call StepMatrix tool as an eventstream method.
    .StepSankey : A class for the visualization of user paths in stepwise manner using Sankey diagram.
    .CollapseLoops : Find loops and create new synthetic events in the paths of all users having such sequences.

    Notes
    -----
    See :doc:`StepMatrix user guide</user_guides/step_matrix>` for the details.
    """

    __eventstream: EventstreamType
    ENDED_EVENT = "ENDED"
    max_steps: int
    weight_col: str
    precision: int
    targets: list[str] | str | None = None
    accumulated: Literal["both", "only"] | None = None
    sorting: list | None = None
    threshold: float
    centered: CenteredParams | None = None
    groups: Tuple[list, list] | None = None

    __result_data: pd.DataFrame
    __result_targets: pd.DataFrame | None
    _fraction_title: str | None
    _targets_list: list[list[str]] | None

    @track(  # type: ignore
        tracking_info={"event_name": "init"},
        scope="step_matrix",
        allowed_params=[
            "max_steps",
            "weight_col",
            "precision",
            "targets",
            "accumulated",
            "sorting",
            "threshold",
            "centered",
            "groups",
        ],
    )
    def __init__(
        self,
        eventstream: EventstreamType,
    ) -> None:
        super().__init__()

        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.event_index_col = self.__eventstream.schema.event_index

        self.__result_data = pd.DataFrame()
        self.__result_targets = None
        self._fraction_title = None
        self._targets_list = None

    def _pad_to_center(self, df_: pd.DataFrame) -> pd.DataFrame | None:
        if self.centered is None:
            return None

        center_event = self.centered.event
        occurrence = self.centered.occurrence
        window = self.centered.left_gap
        position = df_.loc[(df_[self.event_col] == center_event) & (df_["occurrence_counter"] == occurrence)][
            "event_rank"
        ].min()
        shift = position - window - 1
        df_["event_rank"] = df_["event_rank"] - shift
        return df_

    @staticmethod
    def _align_index(df1: pd.DataFrame, df2: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        df1 = df1.align(df2)[0].fillna(0)  # type: ignore
        df2 = df2.align(df1)[0].fillna(0)  # type: ignore
        return df1, df2  # type: ignore

    def _pad_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parameters
        ----------
        df : pd.Dataframe

        Returns
        -------
        pd.Dataframe
            With columns from 0 to ``max_steps``.
        """
        df = df.copy()
        if max(df.columns) < self.max_steps:  # type: ignore
            for col in range(max(df.columns) + 1, self.max_steps + 1):  # type: ignore
                df[col] = 0
        # add missing cols if needed:
        if min(df.columns) > 1:  # type: ignore
            for col in range(1, min(df.columns)):  # type: ignore
                df[col] = 0
        # sort cols
        return df[list(range(1, self.max_steps + 1))]

    def _step_matrix_values(
        self, data: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame | None, str | None, list[list[str]] | None]:
        data = data.copy()

        # ALIGN DATA IF CENTRAL
        if self.centered is not None:
            data, fraction_title = self._center_matrix(data)
        else:
            fraction_title = ""

        # calculate step matrix elements:
        piv = self._generate_step_matrix(data)

        # ADD ROWS FOR TARGETS:
        if self.targets:
            piv_targets, targets = self._process_targets(data)
        else:
            targets, piv_targets = None, None

        return piv, piv_targets, fraction_title, targets

    def _generate_step_matrix(self, data: pd.DataFrame) -> pd.DataFrame:
        agg = data.groupby(["event_rank", self.event_col])[self.weight_col].nunique().reset_index()
        agg[self.weight_col] /= data[self.weight_col].nunique()
        agg = agg[agg["event_rank"] <= self.max_steps]
        agg.columns = ["event_rank", "event_name", "freq"]  # type: ignore
        piv = agg.pivot(index="event_name", columns="event_rank", values="freq").fillna(0)
        # add missing cols if number of events < max_steps:
        piv = self._pad_cols(piv)
        piv.columns.name = None
        piv.index.name = None
        # MAKE TERMINATED STATE ACCUMULATED:
        if self.ENDED_EVENT in piv.index:
            piv.loc[self.ENDED_EVENT] = piv.loc[self.ENDED_EVENT].cumsum().fillna(0)
        return piv

    def _process_targets(self, data: pd.DataFrame) -> tuple[pd.DataFrame | None, list[list[str]] | None]:
        if self.targets is None:
            return None, None

        # format targets to list of lists. E.g. [['a', 'b'], 'c'] -> [['a', 'b'], ['c']]
        targets = []
        if isinstance(self.targets, list):
            for t in self.targets:
                if isinstance(t, list):
                    targets.append(t)
                else:
                    targets.append([t])
        else:
            targets.append([self.targets])

        # obtain flatten list of targets. E.g. [['a', 'b'], 'c'] -> ['a', 'b', 'c']
        targets_flatten = list(itertools.chain(*targets))

        agg_targets = data.groupby(["event_rank", self.event_col])[self.time_col].count().reset_index()
        agg_targets[self.time_col] /= data[self.weight_col].nunique()
        agg_targets.columns = ["event_rank", "event_name", "freq"]  # type: ignore
        agg_targets = agg_targets[agg_targets["event_rank"] <= self.max_steps]
        piv_targets = agg_targets.pivot(index="event_name", columns="event_rank", values="freq").fillna(0)
        piv_targets = self._pad_cols(piv_targets)
        # if target is not present in dataset add zeros:
        for i in targets_flatten:
            if i not in piv_targets.index:
                piv_targets.loc[i] = 0
        piv_targets = piv_targets.loc[targets_flatten].copy()
        piv_targets.columns.name = None
        piv_targets.index.name = None
        ACC_INDEX = "ACC_"

        if self.accumulated == "only":
            piv_targets.index = map(lambda x: ACC_INDEX + x, piv_targets.index)  # type: ignore
            for i in piv_targets.index:
                piv_targets.loc[i] = piv_targets.loc[i].cumsum().fillna(0)

            # change names is targets list:
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = ACC_INDEX + item
        if self.accumulated == "both":
            for i in piv_targets.index:
                piv_targets.loc[ACC_INDEX + i] = piv_targets.loc[i].cumsum().fillna(0)  # type: ignore

            # add accumulated targets to the list:
            targets_not_acc = deepcopy(targets)
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = ACC_INDEX + item
            targets = targets_not_acc + targets
        return piv_targets, targets

    def _center_matrix(self, data: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        if self.centered is None:
            return pd.DataFrame(), ""

        center_event = self.centered.event
        occurrence = self.centered.occurrence
        if center_event not in data[self.event_col].unique():
            error_text = 'Event "{}" from \'centered\' dict not found in the column: "{}"'.format(
                center_event, self.event_col
            )
            raise ValueError(error_text)
        # keep only users who have center_event at least N = occurrence times
        data["occurrence"] = data[self.event_col] == center_event
        data["occurrence_counter"] = data.groupby(self.weight_col)["occurrence"].cumsum() * data["occurrence"]
        users_to_keep = data[data["occurrence_counter"] == occurrence][self.weight_col].unique()
        if len(users_to_keep) == 0:
            raise ValueError(f'no records found with event "{center_event}" occurring N={occurrence} times')

        fraction_used = len(users_to_keep) / data[self.weight_col].nunique() * 100
        if fraction_used < 100:
            fraction_title = f"({fraction_used:.1f}% of total records)"
        else:
            fraction_title = ""
        data = data[data[self.weight_col].isin(users_to_keep)].copy()
        data = data.groupby(self.weight_col).apply(self._pad_to_center)  # type: ignore
        data = data[data["event_rank"] > 0].copy()
        return data, fraction_title

    @staticmethod
    def _sort_matrix(step_matrix: pd.DataFrame) -> pd.DataFrame:
        x = step_matrix.copy()
        order = []
        for i in x.columns:
            new_r = x[i].idxmax()  # type: ignore
            order.append(new_r)
            x = x.drop(new_r)  # type: ignore
            if x.shape[0] == 0:
                break
        order.extend(list(set(step_matrix.index) - set(order)))
        return step_matrix.loc[order]

    def _render_plot(
        self,
        data: pd.DataFrame,
        targets: pd.DataFrame | None,
        targets_list: list[list[str]] | None,
        fraction_title: str | None,
    ) -> matplotlib.axes.Axes:
        n_rows = 1 + (len(targets_list) if targets_list else 0)
        n_cols = 1
        title_part1 = "centered" if self.centered else ""
        title_part2 = "differential " if self.groups else ""
        title = f"{title_part1} {title_part2}step matrix {fraction_title}"
        grid_specs = (
            {"wspace": 0.08, "hspace": 0.04, "height_ratios": [data.shape[0], *list(map(len, targets_list))]}
            if targets is not None and targets_list is not None
            else {}
        )
        figure, axs = sns.mpl.pyplot.subplots(
            n_rows,
            n_cols,
            sharex=True,
            figsize=(
                round(data.shape[1] * 0.7),
                round((len(data) + (len(targets) if targets is not None else 0)) * 0.6),
            ),
            gridspec_kw=grid_specs,
        )
        heatmap = sns.heatmap(
            data,
            yticklabels=data.index,
            annot=True,
            fmt=f".{self.precision}f",
            ax=axs[0] if targets is not None else axs,
            cmap="RdGy",
            center=0,
            cbar=False,
        )
        heatmap.set_title(title, fontsize=16)
        if targets is not None and targets_list is not None:
            target_cmaps = itertools.cycle(["BrBG", "PuOr", "PRGn", "RdBu"])
            for n, i in enumerate(targets_list):
                sns.heatmap(
                    targets.loc[i],
                    yticklabels=targets.loc[i].index,
                    annot=True,
                    fmt=f".{self.precision}f",
                    ax=axs[1 + n],
                    cmap=next(target_cmaps),
                    center=0,
                    vmin=targets.loc[i].values.min(),
                    vmax=targets.loc[i].values.max() or 1,
                    cbar=False,
                )

            for ax in axs:
                sns.mpl.pyplot.sca(ax)
                sns.mpl.pyplot.yticks(rotation=0)

                # add vertical lines for central step-matrix
                if self.centered is not None:
                    centered_position = self.centered.left_gap
                    ax.vlines(
                        [centered_position - 0.02, centered_position + 0.98],
                        *ax.get_ylim(),
                        colors="Black",
                        linewidth=0.7,
                    )

        else:
            sns.mpl.pyplot.sca(axs)
            sns.mpl.pyplot.yticks(rotation=0)
            # add vertical lines for central step-matrix
            if self.centered is not None:
                centered_position = self.centered.left_gap
                axs.vlines(
                    [centered_position - 0.02, centered_position + 0.98], *axs.get_ylim(), colors="Black", linewidth=0.7
                )
        return axs

    @track(  # type: ignore
        tracking_info={"event_name": "fit"},
        scope="step_matrix",
        allowed_params=[
            "max_steps",
            "weight_col",
            "precision",
            "targets",
            "accumulated",
            "sorting",
            "threshold",
            "centered",
            "groups",
        ],
    )
    def fit(
        self,
        max_steps: int = 20,
        weight_col: str | None = None,
        precision: int = 2,
        targets: list[str] | str | None = None,
        accumulated: Literal["both", "only"] | None = None,
        sorting: list | None = None,
        threshold: float = 0,
        centered: dict | None = None,
        groups: Tuple[list, list] | None = None,
    ) -> None:
        """
        Calculates the step matrix internal values with the defined parameters.
        Applying ``fit`` method is necessary for the following usage
        of any visualization or descriptive ``StepMatrix`` methods.

        Parameters
        ----------
        max_steps : int, default 20
            Maximum number of steps in ``user path`` to include.
        weight_col : str, optional
            Aggregation column for edge weighting. If ``None``, specified ``user_id``
            from ``eventstream.schema`` will be used. For example, can be specified as
            ``session_id`` if ``eventstream`` has such ``custom_col``.
        precision : int, default 2
            Number of decimal digits after 0 to show as fractions in the ``heatmap``.
        targets : list of str or str, optional
            List of event names to include in the bottom of ``step_matrix`` as individual rows.
            Each specified target will have separate color-coding space for clear visualization.
            `Example: ['product_page', 'cart', 'payment']`

            If multiple targets need to be compared and plotted using the same color-coding scale,
            such targets must be combined in a sub-list.
            `Example: ['product_page', ['cart', 'payment']]`
        accumulated : {"both", "only"}, optional
            Option to include accumulated values for targets.

            - If ``None``, accumulated tartes are not shown.
            - If ``both``, show step values and accumulated values.
            - If ``only``, show targets only as accumulated.
        sorting : list of str, optional
            - If list of event names specified - lines in the heatmap will be shown in
              the passed order.
            - If ``None`` - rows will be ordered according to i`th value (first row,
              where 1st element is max; second row, where second element is max; etc)
        threshold : float, default 0
            Used to remove rare events. Aggregates all rows where all values are
            less than the specified threshold.
        centered : dict, optional
            Parameter used to align user paths at a specific event at a specific step.
            Has to contain three keys:
            - ``event``: str, name of event to align.
            - ``left_gap``: int, number of events to include before specified event.
            - ``occurrence`` : int which occurrence of event to align (typical 1).

            If not ``None`` - only users who have selected events with the specified
            ``occurrence`` in their paths will be included.
            ``Fraction`` of such remaining users is specified in the title of centered
            step_matrix.
            `Example: {'event': 'cart', 'left_gap': 8, 'occurrence': 1}`
        groups : tuple[list, list], optional
            Can be specified to plot differential step_matrix. Must contain
            a tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
            of user_id`s. Two separate step_matrices M1 and M2 will be calculated
            for users from g_1 and g_2, respectively. Resulting matrix will be the matrix
            M = M1-M2.

        Notes
        -----
        During step matrix calculation an artificial ``ENDED`` event is created. If a path already
        contains ``path_end`` event (See :py:class:`.AddStartEndEvents`), it
        will be temporarily replaced with ``ENDED`` (within step matrix only). Otherwise, ``ENDED``
        event will be explicitly added to the end of each path.

        Event ``ENDED`` is cumulated so that the values in its row are summed up from
        the first step to the last. ``ENDED`` row is always placed at the last line of step matrix.
        This design guarantees that the sum of any step matrix's column is 1
        (0 for a differential step matrix).

        """
        self.max_steps = max_steps
        self.precision = precision
        self.targets = targets
        self.accumulated = accumulated
        self.sorting = sorting
        self.threshold = threshold
        self.centered = CenteredParams(**centered) if centered else None
        self.groups = groups
        self.weight_col = weight_col or self.__eventstream.schema.user_id
        weight_col = self.weight_col or self.user_col
        data = self.__eventstream.to_dataframe()

        data = self._add_ended_events(data=data, schema=self.__eventstream.schema, weight_col=self.weight_col)
        data["event_rank"] = data.groupby(weight_col).cumcount() + 1

        # BY HERE WE NEED TO OBTAIN FINAL DIFF piv and piv_targets before sorting, thresholding and plotting:

        if self.groups:
            data_pos = data[data[weight_col].isin(self.groups[0])].copy()
            if len(data_pos) == 0:
                raise IndexError("Users from positive group are not present in dataset")
            piv_pos, piv_targets_pos, fraction_title, targets_plot = self._step_matrix_values(data=data_pos)

            data_neg = data[data[weight_col].isin(self.groups[1])].copy()
            if len(data_pos) == 0:
                raise IndexError("Users from negative group are not present in dataset")
            piv_neg, piv_targets_neg, fraction_title, targets_plot = self._step_matrix_values(data=data_neg)

            piv_pos, piv_neg = self._align_index(piv_pos, piv_neg)
            piv = piv_pos - piv_neg

            if self.targets and piv_targets_pos and piv_targets_neg:
                piv_targets_pos, piv_targets_neg = self._align_index(piv_targets_pos, piv_targets_neg)
                piv_targets = piv_targets_pos - piv_targets_neg
            else:
                piv_targets = None

        else:
            piv, piv_targets, fraction_title, targets_plot = self._step_matrix_values(data=data)

        threshold_index = "THRESHOLDED_"

        if self.threshold != 0:
            # find if there are any rows to threshold:
            thresholded = piv.loc[(piv.abs() < self.threshold).all(axis=1) & (piv.index != self.ENDED_EVENT)].copy()
            if len(thresholded) > 0:
                piv = piv.loc[(piv.abs() >= self.threshold).any(axis=1) | (piv.index == self.ENDED_EVENT)].copy()
                threshold_index = f"{threshold_index}{len(thresholded)}"
                piv.loc[threshold_index] = thresholded.sum()

        if self.sorting is None:
            piv = self._sort_matrix(piv)

            keep_in_the_end = []
            keep_in_the_end.append(self.ENDED_EVENT) if (self.ENDED_EVENT in piv.index) else None
            keep_in_the_end.append(threshold_index) if (threshold_index in piv.index) else None

            events_order = [*(i for i in piv.index if i not in keep_in_the_end), *keep_in_the_end]
            piv = piv.loc[events_order]

        else:
            if {*self.sorting} != {*piv.index}:
                raise ValueError(
                    "The sorting list provided does not match the list of events. "
                    "Run with `sorting` = None to get the actual list"
                )

            piv = piv.loc[self.sorting]

        if self.centered:
            window = self.centered.left_gap
            piv.columns = [f"{int(i) - window - 1}" for i in piv.columns]  # type: ignore
            if self.targets and piv_targets is not None:
                piv_targets.columns = [f"{int(i) - window - 1}" for i in piv_targets.columns]  # type: ignore

        self.__result_data = piv
        self.__result_targets = piv_targets
        self._fraction_title = fraction_title
        self._targets_list = targets_plot

    @track(  # type: ignore
        tracking_info={"event_name": "plot"},
        scope="step_matrix",
    )
    def plot(self) -> matplotlib.axes.Axes:
        """
        Create a heatmap plot based on the calculated step matrix values.
        Should be used after :py:func:`fit`.

        Returns
        -------
        matplotlib.axes.Axes

        """
        axes = self._render_plot(self.__result_data, self.__result_targets, self._targets_list, self._fraction_title)
        return axes

    @property
    def values(self) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """
        Returns the calculated step matrix as a pd.DataFrame.
        Should be used after :py:func:`fit`.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame | None]
            1. Stands for the step matrix.
            2. Stands for a separate step matrix related for target events only.
        """
        return self.__result_data, self.__result_targets

    @property
    def params(self) -> dict:
        """
        Returns the parameters used for the last fitting.
        Should be used after :py:func:`fit`.

        """
        return {
            "max_steps": self.max_steps,
            "weight_col": self.weight_col,
            "precision": self.precision,
            "targets": self.targets,
            "accumulated": self.accumulated,
            "sorting": self.sorting,
            "threshold": self.threshold,
            "centered": self.centered,
            "groups": self.groups,
        }
