from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.eventstream.types import EventstreamType


class Sankey:
    # TODO: update the doc
    """
    It is function for plotting custom sankey diagram

    Parameters
    ----------
    eventstream : pandas Dataframe
        A preprocessed dataframe which includes event_rank, next_event and time_to_next columns
    max_steps : int
        A number of steps (ranked events) that you want to see at the diagram
    sorting : list
        A custom labels order
    """

    def __init__(
        self,
        eventstream: EventstreamType,
        max_steps: int = 5,
        thresh: Union[int, float] = 0.0,
        sorting: list | None = None,
        target: Union[list[str], str] | None = None,
        autosize: bool | None = True,
        width: int | None = None,
        height: int | None = None,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.data = self.__eventstream.to_dataframe()
        self.max_steps = max_steps
        self.thresh = thresh
        self.sorting = sorting
        self.target = target
        self.autosize = autosize
        self.width = width
        self.height = height

    @staticmethod
    def _make_color(
        event: str,
        all_events: list,
        palette: list,
    ) -> np.array:
        """
        It is color picking function

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
        picked color : str
            A picked color for certain event
        """

        return palette[list(all_events).index(event)]

    @staticmethod
    def _round_up(
        n: float,
        dec: float,
    ) -> float:
        """
        It is decimal rounding up function

        Parameters
        ----------
        n : float
            A number to round up
        dec : float
            A decimal for correct rounding up

        Returns
        -------
        digit : float
            A picked color for certain event
        """

        return round(n - n % dec + dec, 2)

    def _get_nodes_positions(
        self,
        df: pd.DataFrame,
        event_col: str,
    ) -> np.array:
        """
        It is function for placing nodes at the x and y positions of plotly lib plot surface

        Parameters
        ----------
        df : pandas Dataframe
            A dataframe that consist aggregated information about nodes

        Returns
        -------
        digit : numpy array
            A picked color for certain event
        """

        # NOTE get x axis length
        x_len = len(df["event_rank"].unique())

        # NOTE declare positions
        x_positions = []
        y_positions = []
        # NOTE get maximum range for placing middle points
        y_range = 0.95 - 0.05

        # NOTE going inside ranked events
        for event_rank in sorted(df["event_rank"].unique()):

            # NOTE placing x-axis points as well
            for _ in df[df["event_rank"] == event_rank][event_col]:
                x_positions.append([round(x, 2) for x in np.linspace(0.05, 0.95, x_len)][event_rank - 1])

            # NOTE it always works very well if you have less than 4 values at current rank
            y_len = len(df[df["event_rank"] == event_rank][event_col])

            # NOTE at this case using came positions as x-axis because we don't need to calculate something more
            if y_len < 4:

                for p in [round(y, 2) for y in np.linspace(0.05, 0.95, y_len)]:
                    y_positions.append(p)

            # NOTE jumping in to complex part
            else:

                # NOTE total sum for understanding do we need extra step size or not
                total_sum = df[df["event_rank"] == event_rank]["usr_cnt"].sum()
                # NOTE step size for middle points
                step = round(y_range / total_sum, 2)
                # NOTE cumulative sum for understanding do we need use default step size or not
                cumulative_sum = 0
                # NOTE path_end action
                ended_sum = df[(df["event_rank"] == event_rank) & (df[event_col] == "path_end")]["usr_cnt"].sum()
                last_point = self._round_up(ended_sum / total_sum, 0.05)

                # NOTE going deeper inside each event
                for n, event in enumerate(df[df["event_rank"] == event_rank][event_col]):

                    # NOTE placing first event at first possible position
                    if n == 0:

                        y_positions.append(0.05)

                    # NOTE placing last event at last possible position
                    elif n + 1 == y_len:

                        y_positions.append(0.95)

                    # NOTE placing middle points
                    else:

                        # NOTE we found out that 70% of total sum is the best cap for doing this case
                        if iterate_sum / total_sum > 0.2 and event != "path_end":

                            # NOTE placing first point after the biggest one at the next position
                            # but inside [.1; .3] range
                            y_positions.append(
                                round(y_positions[-1] + np.minimum(np.maximum(step * iterate_sum, 0.1), 0.3), 2)
                            )

                        # NOTE placing points after the biggest
                        else:

                            # NOTE placing little points at the all available space
                            y_positions.append(
                                round(y_positions[-1] + (0.95 - last_point - y_positions[-1]) / (y_len - n), 2)
                            )

                    # NOTE set sum for next step
                    iterate_sum = df[(df["event_rank"] == event_rank) & (df[event_col] == event)]["usr_cnt"].to_numpy()[
                        0
                    ]
                    # NOTE update cumulative sum
                    cumulative_sum += iterate_sum

        return x_positions, y_positions

    def _pad_end_events(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        If the number of events in a user's path is less than self.max_steps, then the function pads the path with
        multiplied path_end events. It is required for correct visualization of the trajectories which are
        shorter than self.max_steps.
        """
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_name
        event_index_col = self.__eventstream.schema.event_index
        pad = (
            data.groupby(user_col, as_index=False)[event_col]
            .count()
            .loc[lambda df_: df_[event_col] < self.max_steps]
            .assign(repeat_number=lambda df_: self.max_steps - df_[event_col])
        )
        repeats = pd.DataFrame({user_col: np.repeat(pad[user_col], pad["repeat_number"])})
        padded_end_events = pd.merge(repeats, data[data[event_col] == "path_end"], on=user_col)
        result = pd.concat([data, padded_end_events]).sort_values([user_col, event_index_col])
        return result

    def _prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        event_col = self.__eventstream.schema.event_name
        time_col = self.__eventstream.schema.event_timestamp
        user_col = self.__eventstream.schema.user_id
        event_index_col = self.__eventstream.schema.event_index

        data = self._pad_end_events(data)
        # NOTE set new columns using declared functions
        data[time_col] = pd.to_datetime(data[time_col])
        data["event_rank"] = data.groupby(user_col)[event_index_col].rank(method="first").astype(int)
        data = data.sort_values(by=["event_rank", time_col]).reset_index(drop=True)
        data = self._get_next_event_and_timedelta(data)

        # NOTE threshold
        data["detail"] = data.groupby(by=["event_rank", event_col])[user_col].transform("nunique")
        data["total"] = data.loc[data["event_rank"] == 1, user_col].nunique()
        data["perc"] = data["detail"] / data["total"]

        if self.thresh is not None:
            if isinstance(self.thresh, int):
                data.loc[data["detail"] <= self.thresh, event_col] = "thresholded"
            elif isinstance(self.thresh, float):
                events_to_keep = ["path_end"]
                if self.target is not None:
                    events_to_keep += self.target

                thresh_events = (
                    data.loc[data["event_rank"] <= self.max_steps, :]
                    .groupby(by=event_col, as_index=False)["perc"]
                    .max()
                    .loc[lambda df_: (df_["perc"] <= self.thresh) & (~df_[event_col].isin(events_to_keep))]
                    .loc[:, event_col]
                )
                data.loc[data[event_col].isin(thresh_events), event_col] = f"thresholded_{len(thresh_events)}"

            # NOTE rearrange the data taking into account recently added thresholded events
            data["event_rank"] = data.groupby(user_col)[event_index_col].rank(method="first").astype(int)
            data = self._get_next_event_and_timedelta(data)

        # NOTE use max_steps for filtering data
        data = data.loc[data["event_rank"] <= self.max_steps, :]

        # TODO: Do we really need to replace NA values?
        # NOTE skip mean calculating error
        data["time_to_next"].fillna(data["time_to_next"].min(), inplace=True)
        return data

    def _render_plot(self, data_for_plot: dict, data_grp_nodes: pd.DataFrame) -> go.Figure:
        event_col = self.__eventstream.schema.event_name

        # NOTE fill lists for plot
        targets = []
        sources = []
        values = []
        time_to_next = []
        for source_key, _ in data_for_plot["links_dict"].items():

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
        for key, _ in data_for_plot["nodes_dict"].items():
            labels += list(data_for_plot["nodes_dict"][key]["sources"])
            colors += list(data_for_plot["nodes_dict"][key]["color"])
            percs += list(data_for_plot["nodes_dict"][key]["percs"])
        # NOTE get colors for plot
        for idx, color in enumerate(colors):
            colors[idx] = "rgb" + str(color) + ""
        # NOTE get positions for plot
        x, y = self._get_nodes_positions(df=data_grp_nodes, event_col=event_col)
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
        fig.show()

        return fig

    def _get_links(
        self, data: pd.DataFrame, data_for_plot: dict, data_grp_nodes: pd.DataFrame
    ) -> tuple[dict, pd.DataFrame]:
        event_col = self.__eventstream.schema.event_name
        user_col = self.__eventstream.schema.user_id

        # NOTE create links aggregated dataframe
        data_grp_links = (
            data[data["event_rank"] <= self.max_steps - 1]
            .groupby(by=["event_rank", event_col, "next_event"])[[user_col, "time_to_next"]]
            .agg({user_col: ["count"], "time_to_next": ["sum"]})
            .reset_index()
            .rename(columns={user_col: "usr_cnt", "time_to_next": "time_to_next_sum"})
        )
        data_grp_links.columns = data_grp_links.columns.droplevel(1)
        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["event_rank", event_col, "index"]],
            how="inner",
            on=["event_rank", event_col],
        )
        data_grp_links.loc[:, "next_event_rank"] = data_grp_links["event_rank"] + 1
        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["event_rank", event_col, "index"]].rename(
                columns={"event_rank": "next_event_rank", event_col: "next_event", "index": "next_index"}
            ),
            how="inner",
            on=["next_event_rank", "next_event"],
        )
        data_grp_links.sort_values(
            by=["index", "usr_cnt"],
            ascending=[True, False],
            inplace=True,
        )
        data_grp_links.reset_index(
            drop=True,
            inplace=True,
        )
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
                                next_index: dict(
                                    {
                                        "unique_users": _unique_users[0],
                                        "avg_time_to_next": np.timedelta64(_avg_time_to_next[0]),
                                    }
                                )
                            }
                        )

                else:

                    data_for_plot["links_dict"].update(
                        {
                            index: dict(
                                {
                                    next_index: dict(
                                        {
                                            "unique_users": _unique_users[0],
                                            "avg_time_to_next": np.timedelta64(_avg_time_to_next[0]),
                                        }
                                    )
                                }
                            )
                        }
                    )
        return data_for_plot, data_grp_links

    def _get_nodes(self, data: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
        event_col = self.__eventstream.schema.event_name
        user_col = self.__eventstream.schema.user_id

        all_events = list(data[event_col].unique())

        # NOTE default color palette
        palette = ["50BE97", "E4655C", "FCC865", "BFD6DE", "3E5066", "353A3E", "E6E6E6"]
        # NOTE convert HEX to RGB
        for i, col in enumerate(palette):
            palette[i] = tuple(int(col[i : i + 2], 16) for i in (0, 2, 4))
        # NOTE extend color palette if number of events more than default colors list
        complementary_palette = sns.color_palette("deep", len(all_events) - len(palette))
        if len(complementary_palette) > 0:
            colors = complementary_palette.as_hex()

            for c in colors:
                col = c[1:]
                palette.append(tuple(int(col[i : i + 2], 16) for i in (0, 2, 4)))
        # NOTE create nodes aggregate dataframe
        data_grp_nodes = (
            data.groupby(by=["event_rank", event_col])[user_col]
            .nunique()
            .reset_index()
            .rename(columns={user_col: "usr_cnt"})
        )
        data_grp_nodes.loc[:, "usr_cnt_total"] = data_grp_nodes.groupby(by=["event_rank"])["usr_cnt"].transform("sum")
        data_grp_nodes.loc[:, "perc"] = round(
            (data_grp_nodes.loc[:, "usr_cnt"] / data_grp_nodes.loc[:, "usr_cnt_total"]) * 100, 2
        )
        data_grp_nodes.sort_values(
            by=["event_rank", "usr_cnt", event_col],
            ascending=[True, False, True],
            inplace=True,
        )
        data_grp_nodes.reset_index(
            drop=True,
            inplace=True,
        )
        data_grp_nodes.loc[:, "color"] = data_grp_nodes[event_col].apply(
            lambda x: self._make_color(x, all_events, palette)
        )
        data_grp_nodes.loc[:, "index"] = data_grp_nodes.index
        # NOTE doing right ranking
        if self.sorting is None:
            data_grp_nodes.loc[:, "sorting"] = 100
        else:
            for n, s in enumerate(self.sorting):
                data_grp_nodes.loc[data_grp_nodes[event_col] == s, "sorting"] = n
            data_grp_nodes.loc[:, "sorting"].fillna(100, inplace=True)
        # NOTE placing path_end at the end
        data_grp_nodes.loc[data_grp_nodes[event_col] == "path_end", "sorting"] = 101
        # NOTE using custom ordering
        data_grp_nodes.loc[:, "sorting"] = data_grp_nodes.loc[:, "sorting"].astype(int)
        # TODO: event_rank is not used inside the loop. Seems like the loop is invalid.
        # NOTE doing loop for valid ranking
        for event_rank in data_grp_nodes["event_rank"].unique():
            # NOTE saving last level order
            data_grp_nodes.loc[:, "order_by"] = (
                data_grp_nodes.groupby(by=[event_col])["index"].transform("shift").fillna(100).astype(int)
            )

            # NOTE placing path_end events at the end
            data_grp_nodes.loc[data_grp_nodes[event_col] == "path_end", "sorting"] = 101

            # NOTE creating new indexes
            data_grp_nodes.sort_values(
                by=["event_rank", "sorting", "order_by", "usr_cnt", event_col],
                ascending=[True, True, True, False, True],
                inplace=True,
            )

            data_grp_nodes.reset_index(
                drop=True,
                inplace=True,
            )

            data_grp_nodes.loc[:, "index"] = data_grp_nodes.index

        # NOTE generating nodes plot dict
        data_for_plot = dict()
        data_for_plot.update({"nodes_dict": dict()})
        for event_rank in data_grp_nodes["event_rank"].unique():
            data_for_plot["nodes_dict"].update({event_rank: dict()})
            _sources, _color, _sources_index, _percs = (
                data_grp_nodes.loc[data_grp_nodes["event_rank"] == event_rank, [event_col, "color", "index", "perc"]]
                .to_numpy()
                .T
            )

            data_for_plot["nodes_dict"][event_rank].update(
                {
                    "sources": list(_sources),
                    "color": list(_color),
                    "sources_index": list(_sources_index),
                    "percs": list(_percs),
                }
            )

        return data_for_plot, data_grp_nodes

    def _get_next_event_and_timedelta(self, data):
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_name
        time_col = self.__eventstream.schema.event_timestamp

        grouped = data.groupby(user_col)
        data["next_event"] = grouped[event_col].shift(-1)
        data["next_timestamp"] = grouped[time_col].shift(-1)
        data["time_to_next"] = data["next_timestamp"] - data[time_col]
        data = data.drop("next_timestamp", axis=1)
        return data

    def _get_plot_data(self) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_name
        time_col = self.__eventstream.schema.event_timestamp
        event_index_col = self.__eventstream.schema.event_index
        data = self.__eventstream.to_dataframe().copy()[[user_col, event_col, time_col, event_index_col]]
        data = self._prepare_data(data)
        data_for_plot, data_grp_nodes = self._get_nodes(data)
        data_for_plot, data_grp_links = self._get_links(data, data_for_plot, data_grp_nodes)
        return data_for_plot, data_grp_nodes, data_grp_links, data

    def plot(self) -> go.Figure:
        data_for_plot, data_grp_nodes, data_grp_links, _ = self._get_plot_data()
        figure = self._render_plot(data_for_plot, data_grp_nodes)
        return figure
