from __future__ import annotations

import itertools
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal, Optional, Tuple, Union

import matplotlib
import pandas as pd
import seaborn as sns

from src.eventstream.types import EventstreamType


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


class StepMatrix:
    """
    Plots heatmap with distribution of users over trajectory steps ordered by
    event name. Matrix rows are event names, columns are aligned user trajectory
    step numbers and the values are shares of users. A given entry X at column i
    and event j means at i'th step fraction of users X  have specific event j.
    Parameters
    ----------
    max_steps: int (optional, default 20)
        Maximum number of steps in trajectories to include.
    weight_col: str (optional, default None)
        Aggregation column for edge weighting. If None, specified index_col
        from retentioneering.config will be used as column name. For example,
        can be specified as `session_id` if dataframe has such column.
    precision: int (optional, default 2)
        Number of decimal digits after 0 to show as fractions in the heatmap.
    thresh: float (optional, default 0)
        Used to remove rare events. Aggregates all rows where all values are
        less then specified threshold.
    targets: list (optional, default None)
        List of events names (as str) to include in the bottom of
        step_matrix as individual rows. Each specified target will have
        separate color-coding space for clear visualization. Example:
        ['product_page', 'cart', 'payment']. If multiple targets need to
        be compared and plotted using same color-coding scale, such targets
        must be combined in sub-list.
        Examples: ['product_page', ['cart', 'payment']]
    accumulated: string (optional, default None)
        Option to include accumulated values for targets. Valid values are
        None (do not show accumulated tartes), 'both' (show step values and
        accumulated values), 'only' (show targets only as accumulated).
    centered: dict (optional, default None)
        Parameter used to align user trajectories at specific event at specific
        step. Has to contain three keys:
            'event': str, name of event to align
            'left_gap': int, number of events to include before specified event
            'occurrence': int which occurrence of event to align (typical 1)
        When this parameter is not None only users which have specified i'th
        'occurrence' of selected event preset in their trajectories will
        be included. Fraction of such remaining users is specified in the title of
        centered step_matrix. Example:
        {'event': 'cart', 'left_gap': 8, 'occurrence': 1}
    sorting: list (optional, default None)
        List of events_names (as string) can be passed to plot step_matrix with
        specified ordering of events. If None rows will be ordered according
        to i`th value (first row, where 1st element is max, second row, where
        second element is max, etc)
    groups: tuple (optional, default None)
        Can be specified to plot step differential step_matrix. Must contain
        tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
        of user_id`s (list, tuple or set). Two separate step_matrices M1 and M2
        will be calculated for users from g_1 and g_2, respectively. Resulting
        matrix will be the matrix M = M1-M2. Note, that values in each column
        in differential step matrix will sum up to 0 (since columns in both M1
        and M2 always sum up to 1).

    Returns
    -------
    Dataframe with max_steps number of columns and len(event_col.unique)
    number of rows at max, or less if used thr > 0.
    Return type
    -----------
    pd.DataFrame
    """

    __eventstream: EventstreamType

    def __init__(
        self,
        eventstream: EventstreamType,
        max_steps: int = 20,
        weight_col: Optional[str] = None,
        precision: int = 2,
        targets: Optional[list[str] | str] = None,
        accumulated: Optional[Union[Literal["both", "only"], None]] = None,
        sorting: Optional[list[str]] = None,
        thresh: float = 0,
        centered: Optional[dict] = None,
        groups: Optional[Tuple[list, list]] = None,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.data = self.__eventstream.to_dataframe()
        self.max_steps = max_steps
        self.weight_col = weight_col or self.__eventstream.schema.user_id
        self.precision = precision
        self.targets = targets
        self.accumulated = accumulated
        self.sorting = sorting
        self.thresh = thresh
        self.centered: CenteredParams | None = CenteredParams(**centered) if centered else None
        self.groups = groups

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
        df - dataframe
        Returns
        -------
        returns Dataframe with columns from 0 to max_steps
        """
        df = df.copy()
        if max(df.columns) < self.max_steps:
            for col in range(max(df.columns) + 1, self.max_steps + 1):
                df[col] = 0
        # add missing cols if needed:
        if min(df.columns) > 1:
            for col in range(1, min(df.columns)):
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
        if "path_end" in piv.index:
            piv.loc["path_end"] = piv.loc["path_end"].cumsum().fillna(0)
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
        if self.accumulated == "only":
            piv_targets.index = map(lambda x: "ACC_" + x, piv_targets.index)  # type: ignore
            for i in piv_targets.index:
                piv_targets.loc[i] = piv_targets.loc[i].cumsum().fillna(0)

            # change names is targets list:
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = "ACC_" + item
        if self.accumulated == "both":
            for i in piv_targets.index:
                piv_targets.loc["ACC_" + i] = piv_targets.loc[i].cumsum().fillna(0)

            # add accumulated targets to the list:
            targets_not_acc = deepcopy(targets)
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = "ACC_" + item
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
            new_r = x[i].idxmax()
            order.append(new_r)
            x = x.drop(new_r)
            if x.shape[0] == 0:
                break
        order.extend(list(set(step_matrix.index) - set(order)))
        return step_matrix.loc[order]

    def _render_plot(
        self,
        data: pd.DataFrame,
        fraction_title: str | None,
        targets: pd.DataFrame | None,
        targets_list: list[list[str]] | None,
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
        f, axs = sns.mpl.pyplot.subplots(
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
                    vmin=min(itertools.chain(targets.loc[i])),
                    vmax=max(itertools.chain(targets.loc[i])) or 1,
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

    def _get_plot_data(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None, str | None, list[list[str]] | None]:
        weight_col = self.weight_col or self.user_col
        data = self.__eventstream.to_dataframe()
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

        thresh_index = "THRESHOLDED_"
        if self.thresh != 0:
            # find if there are any rows to threshold:
            thresholded = piv.loc[(piv.abs() < self.thresh).all(axis=1)].copy()
            if len(thresholded) > 0:
                piv = piv.loc[(piv.abs() >= self.thresh).any(axis=1)].copy()
                thresh_index = f"THRESHOLDED_{len(thresholded)}"
                piv.loc[thresh_index] = thresholded.sum()

        if self.sorting is None:
            piv = self._sort_matrix(piv)

            keep_in_the_end = []
            keep_in_the_end.append("path_end") if ("path_end" in piv.index) else None
            keep_in_the_end.append(thresh_index) if (thresh_index in piv.index) else None

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
            piv.columns = [(int(i) - window - 1) for i in piv.columns]  # type: ignore
            if self.targets and piv_targets is not None:
                piv_targets.columns = [(int(i) - window - 1) for i in piv_targets.columns]  # type: ignore

        return piv, piv_targets, fraction_title, targets_plot

    def plot(self) -> matplotlib.axes.Axes:
        data, targets, fraction_title, targets_list = self._get_plot_data()
        return self._render_plot(data, fraction_title, targets, targets_list)
