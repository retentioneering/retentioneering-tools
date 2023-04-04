from __future__ import annotations

from typing import Any, Dict, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.mixins.ended_events import EndedEventsMixin


class StepSankey(EndedEventsMixin):
    """
    A class for the visualization of user paths in stepwise manner using Sankey diagram.

    Parameters
    ----------
    eventstream : EventstreamType
    max_steps : int, default 10
        Maximum number of steps in trajectories to include. Should be > 1.
    thresh : float | int, default 0.05
        Used to remove rare events from the plot. An event is collapsed to ``thresholded_N`` artificial event if
        its maximum frequency across all the steps is less than or equal to ``thresh``. The frequency is set
        with respect to ``thresh`` type:

        - If ``int`` - the frequency is the number of unique users who had given event at given step.
        - If ``float`` - percentage of users: the same as for ``int``, but divided by the number of unique users.

        The events which are prohibited for collapsing could be enlisted in ``target`` parameter.
    sorting : list of str, optional
        Define the order of the events visualized at each step. The events that are not represented in the list
        will follow after the events from the list.
    target : list of str, optional
        Contain events that are prohibited for collapsing with ``thresh`` parameter.
    autosize : bool, default True
        Plotly autosize parameter. See :plotly_autosize:`plotly documentation<>`.
    width : int, optional
        Plot's width (in px). See :plotly_width:`plotly documentation<>`.
    height : int, optional
        Plot's height (in px). See :plotly_height:`plotly documentation<>`.

    Raises
    ------
    ValueError
        If ``max_steps`` parameter is <= 1.

    See Also
    --------
    .Eventstream.step_sankey : Call StepSankey tool as an eventstream method.
    .CollapseLoops : Find loops and create new synthetic events in the paths of all users having such sequences.
    .StepMatrix : This class provides methods for step matrix calculation and visualization.

    Notes
    -----
    See :doc:`StepSankey user guide</user_guides/step_sankey>` for the details.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        max_steps: int = 10,
        thresh: Union[int, float] = 0.05,
        sorting: list | None = None,
        target: Union[list[str], str] | None = None,
        autosize: bool = True,
        width: int | None = None,
        height: int | None = None,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.event_index_col = self.__eventstream.schema.event_index

        self.max_steps = max_steps
        self.thresh = thresh
        self.sorting = sorting
        self.target = target
        self.autosize = autosize
        self.width = width
        self.height = height

        self.data_grp_nodes: pd.DataFrame = pd.DataFrame()
        self.data: pd.DataFrame = pd.DataFrame()
        self.data_grp_links: pd.DataFrame = pd.DataFrame()
        self.data_for_plot: dict = {}
        if max_steps <= 1:
            raise ValueError("max_steps parameter must be > 1!")

    @staticmethod
    def _make_color(
        event: str,
        all_events: list,
        palette: list,
    ) -> tuple[int, int, int]:
        """
        It is a color picking function

        Parameters
        ----------
        event : str
            An event for color setting
        all_events : list
            A list of all events
        palette : list
            A list of colors

        Returns
        -------
        str
            A picked color for certain event
        """

        return palette[list(all_events).index(event)]

    @staticmethod
    def _round_up(
        n: float,
        dec: float,
    ) -> float:
        """
        Rounds the value up to the nearest value assuming a grid with ``dec`` step.
        E.g. ``_round_up(0.51, 0.05) = 0.55``, ``_round_up(0.55, 0.05) = 0.6``

        Parameters
        ----------
        n : float
            A number to round up
        dec : float
            A decimal for correct rounding up

        Returns
        -------
        float
            Rounded value
        """

        return round(n - n % dec + dec, 2)

    def _get_nodes_positions(self, df: pd.DataFrame) -> tuple[list[float], list[float]]:
        """
        It is a function for placing nodes at the x and y coordinates of plotly lib plot canvas.

        Parameters
        ----------
        df : pandas Dataframe
            A dataframe that contains aggregated information about the nodes.

        Returns
        -------
        tuple[list[float], list[float]]
            Two lists with the corresponding coordinates x and y.
        """
        # NOTE get x axis length
        x_len = len(df["step"].unique())

        # NOTE declare positions
        x_positions = []
        y_positions = []
        # NOTE get maximum range for placing middle points
        y_range = 0.95 - 0.05

        # NOTE going inside ranked events
        for step in sorted(df["step"].unique()):
            # NOTE placing x-axis points as well
            for _ in df[df["step"] == step][self.event_col]:
                x_positions.append([round(x, 2) for x in np.linspace(0.05, 0.95, x_len)][step - 1])

            # NOTE it always works very well if you have less than 4 values at current rank
            y_len = len(df[df["step"] == step][self.event_col])

            # NOTE at this case using came positions as x-axis because we don't need to calculate something more
            if y_len < 4:
                for p in [round(y, 2) for y in np.linspace(0.05, 0.95, y_len)]:
                    y_positions.append(p)

            # NOTE jumping in to complex part
            else:
                # NOTE total sum for understanding do we need extra step size or not
                total_sum = df[df["step"] == step]["usr_cnt"].sum()
                # NOTE step size for middle points
                y_step = round(y_range / total_sum, 2)
                # NOTE cumulative sum for understanding do we need use default step size or not
                cumulative_sum = 0
                # NOTE ENDED action
                ended_sum = df[(df["step"] == step) & (df[self.event_col] == "ENDED")]["usr_cnt"].sum()
                last_point = self._round_up(ended_sum / total_sum, 0.05)

                iterate_sum = 0

                # NOTE going deeper inside each event
                for n, event in enumerate(df[df["step"] == step][self.event_col]):
                    # NOTE placing first event at first possible position
                    if n == 0:
                        y_positions.append(0.05)

                    # NOTE placing last event at last possible position
                    elif n + 1 == y_len:
                        y_positions.append(0.95)

                    # NOTE placing middle points
                    else:
                        # NOTE we found out that 70% of total sum is the best cap for doing this case
                        if iterate_sum / total_sum > 0.2 and event != "ENDED":
                            # NOTE placing first point after the biggest one at the next position
                            # but inside [.1; .3] range
                            y_positions.append(
                                round(y_positions[-1] + np.minimum(np.maximum(y_step * iterate_sum, 0.1), 0.3), 2)
                            )

                        # NOTE placing points after the biggest
                        else:
                            # NOTE placing little points at the all available space
                            y_positions.append(
                                round(y_positions[-1] + (0.95 - last_point - y_positions[-1]) / (y_len - n), 2)
                            )

                    # NOTE set sum for next step
                    iterate_sum = df[(df["step"] == step) & (df[self.event_col] == event)]["usr_cnt"].to_numpy()[0]
                    # NOTE update cumulative sum
                    cumulative_sum += iterate_sum

        return x_positions, y_positions

    def _pad_end_events(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        If the number of events in a user's path is less than self.max_steps, then the function pads the path with
        multiply ENDED events. It is required for correct visualization of the trajectories which are
        shorter than self.max_steps.
        """
        pad = (
            data.groupby(self.user_col, as_index=False)[self.event_col]
            .count()
            .loc[lambda df_: df_[self.event_col] < self.max_steps]  # type: ignore
            .assign(repeat_number=lambda df_: self.max_steps - df_[self.event_col])
        )
        repeats = pd.DataFrame({self.user_col: np.repeat(pad[self.user_col], pad["repeat_number"])})
        padded_end_events = pd.merge(repeats, data[data[self.event_col] == "ENDED"], on=self.user_col)
        result = pd.concat([data, padded_end_events]).sort_values([self.user_col, self.event_index_col])
        return result

    def _prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._add_ended_events(data, self.__eventstream.schema, self.__eventstream.schema.user_id)
        data = self._pad_end_events(data)
        # NOTE set new columns using declared functions
        data[self.time_col] = pd.to_datetime(data[self.time_col])
        data["step"] = data.groupby(self.user_col)[self.event_index_col].rank(method="first").astype(int)
        data = data.sort_values(by=["step", self.time_col]).reset_index(drop=True)
        data = self._get_next_event_and_timedelta(data)

        # NOTE threshold
        data["event_users"] = data.groupby(by=["step", self.event_col])[self.user_col].transform("nunique")
        data["total_users"] = data.loc[data["step"] == 1, self.user_col].nunique()
        data["perc"] = data["event_users"] / data["total_users"]

        if self.thresh is not None:
            if isinstance(self.thresh, float):
                column_to_compare = "perc"
            else:
                # assume that self.thresh must be of int type here
                column_to_compare = "event_users"

            events_to_keep = ["ENDED"]
            if self.target is not None:
                events_to_keep += self.target

            thresh_events = (
                data.loc[data["step"] <= self.max_steps, :]
                .groupby(by=self.event_col, as_index=False)[column_to_compare]
                .max()
                .loc[
                    lambda df_: (df_[column_to_compare] <= self.thresh) & (~df_[self.event_col].isin(events_to_keep))
                ]  # type: ignore
                .loc[:, self.event_col]
            )
            data.loc[data[self.event_col].isin(thresh_events), self.event_col] = f"thresholded_{len(thresh_events)}"

            # NOTE rearrange the data taking into account recently added thresholded events
            data["step"] = data.groupby(self.user_col)[self.event_index_col].rank(method="first").astype(int)
            data = self._get_next_event_and_timedelta(data)

        # NOTE use max_steps for filtering data
        data = data.loc[data["step"] <= self.max_steps, :]

        # TODO: Do we really need to replace NA values?
        # NOTE skip mean calculating error
        data["time_to_next"].fillna(data["time_to_next"].min(), inplace=True)
        return data

    def _render_plot(self, data_for_plot: dict, data_grp_nodes: pd.DataFrame) -> go.Figure:
        # NOTE fill lists for plot
        targets = []
        sources = []
        values = []
        time_to_next = []
        for source_key in data_for_plot["links_dict"].keys():
            for target_key, target_value in data_for_plot["links_dict"][source_key].items():
                sources.append(source_key)
                targets.append(target_key)
                values.append(target_value["unique_users"])
                time_to_next.append(
                    str(pd.to_timedelta(target_value["avg_time_to_next"] / target_value["unique_users"])).split(".")[0]
                )
        # NOTE fill another lists for plot
        labels = []
        colors = []
        percs = []
        for key in data_for_plot["nodes_dict"].keys():
            labels += list(data_for_plot["nodes_dict"][key]["sources"])
            colors += list(data_for_plot["nodes_dict"][key]["color"])
            percs += list(data_for_plot["nodes_dict"][key]["percs"])
        # NOTE get colors for plot
        for idx, color in enumerate(colors):
            colors[idx] = "rgb" + str(color) + ""
        # NOTE get positions for plot
        x, y = self._get_nodes_positions(df=data_grp_nodes)
        # NOTE make plot
        fig = go.Figure(
            data=[
                go.Sankey(
                    arrangement="snap",
                    node=dict(
                        thickness=15,
                        line=dict(color="black", width=0.5),
                        label=labels,
                        color=colors,
                        customdata=percs,
                        hovertemplate="Total unique users: %{value} (%{customdata}% of total)<extra></extra>",
                        x=x,
                        y=y,
                        pad=20,
                    ),
                    link=dict(
                        source=sources,
                        target=targets,
                        value=values,
                        label=time_to_next,
                        hovertemplate="%{value} unique users went from %{source.label} to %{target.label}.<br />"
                        + "<br />It took them %{label} in average.<extra></extra>",
                    ),
                )
            ]
        )
        fig.update_layout(
            font=dict(size=15), plot_bgcolor="white", autosize=self.autosize, width=self.width, height=self.height
        )

        return fig

    def _get_links(
        self, data: pd.DataFrame, data_for_plot: dict, data_grp_nodes: pd.DataFrame
    ) -> tuple[dict, pd.DataFrame]:
        # NOTE create links aggregated dataframe
        data_grp_links = (
            data[data["step"] <= self.max_steps - 1]
            .groupby(by=["step", self.event_col, "next_event"])[[self.user_col, "time_to_next"]]
            .agg({self.user_col: ["count"], "time_to_next": ["sum"]})
            .reset_index()
            .rename(columns={self.user_col: "usr_cnt", "time_to_next": "time_to_next_sum"})
        )
        data_grp_links.columns = data_grp_links.columns.droplevel(1)
        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["step", self.event_col, "index"]],
            how="inner",
            on=["step", self.event_col],
        )
        data_grp_links.loc[:, "next_step"] = data_grp_links["step"] + 1
        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["step", self.event_col, "index"]].rename(
                columns={"step": "next_step", self.event_col: "next_event", "index": "next_index"}
            ),
            how="inner",
            on=["next_step", "next_event"],
        )

        data_grp_links.sort_values(by=["index", "usr_cnt"], ascending=[True, False], inplace=True)
        data_grp_links.reset_index(drop=True, inplace=True)

        # NOTE generating links plot dict
        data_for_plot.update({"links_dict": dict()})
        for index in data_grp_links["index"].unique():
            for next_index in data_grp_links[data_grp_links["index"] == index]["next_index"].unique():
                _unique_users, _avg_time_to_next = (
                    data_grp_links.loc[
                        (data_grp_links["index"] == index) & (data_grp_links["next_index"] == next_index),
                        ["usr_cnt", "time_to_next_sum"],
                    ]
                    .to_numpy()
                    .T
                )

                if index in data_for_plot["links_dict"]:
                    if next_index in data_for_plot["links_dict"][index]:
                        data_for_plot["links_dict"][index][next_index]["unique_users"] = _unique_users[0]
                        data_for_plot["links_dict"][index][next_index]["avg_time_to_next"] = np.timedelta64(
                            _avg_time_to_next[0]
                        )
                    else:
                        data_for_plot["links_dict"][index].update(
                            {
                                next_index: {
                                    "unique_users": _unique_users[0],
                                    "avg_time_to_next": np.timedelta64(_avg_time_to_next[0]),
                                }
                            }
                        )
                else:
                    data_for_plot["links_dict"].update(
                        {
                            index: {
                                next_index: {
                                    "unique_users": _unique_users[0],
                                    "avg_time_to_next": np.timedelta64(_avg_time_to_next[0]),
                                }
                            }
                        }
                    )
        return data_for_plot, data_grp_links

    def _get_nodes(self, data: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
        all_events = list(data[self.event_col].unique())
        palette = self._prepare_palette(all_events)

        # NOTE create nodes aggregate dataframe
        data_grp_nodes = (
            data.groupby(by=["step", self.event_col])[self.user_col]
            .nunique()
            .reset_index()
            .rename(columns={self.user_col: "usr_cnt"})
        )
        data_grp_nodes.loc[:, "usr_cnt_total"] = data_grp_nodes.groupby(by=["step"])["usr_cnt"].transform("sum")
        data_grp_nodes.loc[:, "perc"] = np.round(
            (data_grp_nodes.loc[:, "usr_cnt"] / data_grp_nodes.loc[:, "usr_cnt_total"]) * 100, 2
        )
        data_grp_nodes.sort_values(
            by=["step", "usr_cnt", self.event_col],
            ascending=[True, False, True],
            inplace=True,
        )
        data_grp_nodes.reset_index(
            drop=True,
            inplace=True,
        )
        data_grp_nodes.loc[:, "color"] = data_grp_nodes[self.event_col].apply(
            lambda x: self._make_color(x, all_events, palette)
        )
        data_grp_nodes.loc[:, "index"] = data_grp_nodes.index  # type: ignore
        # NOTE doing right ranking
        if self.sorting is None:
            data_grp_nodes.loc[:, "sorting"] = 100
        else:
            for n, s in enumerate(self.sorting):
                data_grp_nodes.loc[data_grp_nodes[self.event_col] == s, "sorting"] = n
            data_grp_nodes.loc[:, "sorting"].fillna(100, inplace=True)
        # NOTE placing ENDED at the end
        data_grp_nodes.loc[data_grp_nodes[self.event_col] == "ENDED", "sorting"] = 101
        # NOTE using custom ordering
        data_grp_nodes.loc[:, "sorting"] = data_grp_nodes.loc[:, "sorting"].astype(int)

        # @TODO: step variable is not used inside the loop. The loop might be invalid. Vladimir Kukushkin
        # NOTE doing loop for valid ranking
        for step in data_grp_nodes["step"].unique():
            # NOTE saving last level order
            data_grp_nodes.loc[:, "order_by"] = (
                data_grp_nodes.groupby(by=[self.event_col])["index"].transform("shift").fillna(100).astype(int)
            )

            # NOTE placing ENDED events at the end
            data_grp_nodes.loc[data_grp_nodes[self.event_col] == "ENDED", "sorting"] = 101

            # NOTE creating new indexes
            data_grp_nodes.sort_values(
                by=["step", "sorting", "order_by", "usr_cnt", self.event_col],
                ascending=[True, True, True, False, True],
                inplace=True,
            )

            data_grp_nodes.reset_index(
                drop=True,
                inplace=True,
            )

            data_grp_nodes.loc[:, "index"] = data_grp_nodes.index  # type: ignore

        # NOTE generating nodes plot dict
        data_for_plot: Dict[str, Any] = dict()
        data_for_plot.update({"nodes_dict": dict()})
        for step in data_grp_nodes["step"].unique():
            data_for_plot["nodes_dict"].update({step: dict()})
            _sources, _color, _sources_index, _percs = (
                data_grp_nodes.loc[data_grp_nodes["step"] == step, [self.event_col, "color", "index", "perc"]]
                .to_numpy()
                .T
            )

            data_for_plot["nodes_dict"][step].update(
                {
                    "sources": list(_sources),
                    "color": list(_color),
                    "sources_index": list(_sources_index),
                    "percs": list(_percs),
                }
            )

        return data_for_plot, data_grp_nodes

    @staticmethod
    def _prepare_palette(all_events: list) -> list[tuple]:
        # NOTE default color palette
        palette_hex = ["50BE97", "E4655C", "FCC865", "BFD6DE", "3E5066", "353A3E", "E6E6E6"]
        # NOTE convert HEX to RGB
        palette = []
        for color in palette_hex:
            rgb_color = tuple(int(color[i : i + 2], 16) for i in (0, 2, 4))
            palette.append(rgb_color)

        # NOTE extend color palette if number of events more than default colors list
        complementary_palette = sns.color_palette("deep", len(all_events) - len(palette))
        if len(complementary_palette) > 0:
            colors = complementary_palette.as_hex()
            for c in colors:
                col = c[1:]
                palette.append(tuple(int(col[i : i + 2], 16) for i in (0, 2, 4)))

        return palette

    def _get_next_event_and_timedelta(self, data: pd.DataFrame) -> pd.DataFrame:
        grouped = data.groupby(self.user_col)
        data["next_event"] = grouped[self.event_col].shift(-1)
        data["next_timestamp"] = grouped[self.time_col].shift(-1)
        data["time_to_next"] = data["next_timestamp"] - data[self.time_col]
        data = data.drop("next_timestamp", axis=1)
        return data

    def fit(self) -> None:
        """
        Calculate the sankey diagram internal values with the defined parameters.
        Applying ``fit`` method is necessary for the following usage
        of any visualization or descriptive ``StepSankey`` methods.

        """
        data = self.__eventstream.to_dataframe().copy()[
            [self.user_col, self.event_col, self.time_col, self.event_index_col]
        ]
        self.data = self._prepare_data(data)
        data_for_plot, self.data_grp_nodes = self._get_nodes(self.data)
        self.data_for_plot, self.data_grp_links = self._get_links(self.data, data_for_plot, self.data_grp_nodes)

    def plot(self) -> go.Figure:
        """
        Create a Sankey interactive plot based on the calculated values.
        Should be used after :py:func:`fit`.

        Returns
        -------
        plotly.graph_objects.Figure

        """
        figure = self._render_plot(self.data_for_plot, self.data_grp_nodes)
        return figure

    @property
    def values(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns two pd.DataFrames which the Sankey diagram is based on.

        Should be used after :py:func:`fit`.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            1. Contains the nodes of the diagram.
            2. Contains the edges of the diagram.

        """
        return self.data_grp_nodes, self.data_grp_links

    @property
    def params(self) -> dict:
        """
        Returns the parameters used for the last fitting.
        Should be used after :py:func:`fit`.

        """
        return {
            "max_steps": self.max_steps,
            "thresh": self.thresh,
            "sorting": self.sorting,
            "target": self.target,
            "autosize": self.autosize,
            "width": self.width,
            "height": self.height,
        }
