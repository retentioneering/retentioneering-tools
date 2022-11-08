from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.eventstream.types import EventstreamType


class Sankey:
    """
    It is function for plotting custom sankey diagram

    Parameters
    ----------
    eventstream : pandas Dataframe
        A preprocessed dataframe which includes rank_event, next_event and time_to_next columns
    max_steps : int
        A number of steps (ranked events) that you want to see at the diagram
    sorting : list
        A custom labels order
    """

    def __init__(
        self,
        eventstream: EventstreamType,
        max_steps: int,
        thresh: Union[int, float],
        sorting: list = None,
        target: str = None,
        autosize: bool = True,
        width: int = None,
        height: int = None,
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
        x_len = len(df["rank_event"].unique())

        # NOTE declare positions
        x_positions = []
        y_positions = []
        # NOTE get maximum range for placing middle points
        y_range = 0.95 - 0.05

        # NOTE going inside ranked events
        for rank_event in sorted(df["rank_event"].unique()):

            # NOTE placing x-axis points as well
            for _ in df[df["rank_event"] == rank_event][event_col]:
                x_positions.append([round(x, 2) for x in np.linspace(0.05, 0.95, x_len)][rank_event - 1])

            # NOTE it always works very well if you have less than 4 values at current rank
            y_len = len(df[df["rank_event"] == rank_event][event_col])

            # NOTE at this case using came positions as x-axis because we don't need to calculate something more
            if y_len < 4:

                for p in [round(y, 2) for y in np.linspace(0.05, 0.95, y_len)]:
                    y_positions.append(p)

            # NOTE jumping in to complex part
            else:

                # NOTE total sum for understanding do we need extra step size or not
                total_sum = df[df["rank_event"] == rank_event]["usr_cnt"].sum()
                # NOTE step size for middle points
                step = round(y_range / total_sum, 2)
                # NOTE cumulative sum for understanding do we need use default step size or not
                cumulative_sum = 0
                # NOTE ended action
                ended_sum = df[(df["rank_event"] == rank_event) & (df[event_col] == "ended")]["usr_cnt"].sum()
                last_point = self._round_up(ended_sum / total_sum, 0.05)

                # NOTE going deeper inside each event
                for n, event in enumerate(df[df["rank_event"] == rank_event][event_col]):

                    # NOTE placing first event at first possible position
                    if n == 0:

                        y_positions.append(0.05)

                    # NOTE placing last event at last possible position
                    elif n + 1 == y_len:

                        y_positions.append(0.95)

                    # NOTE placing middle points
                    else:

                        # NOTE we found out that 70% of total sum is the best cap for doing this case
                        if iterate_sum / total_sum > 0.2 and event != "ended":

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
                    iterate_sum = df[(df["rank_event"] == rank_event) & (df[event_col] == event)]["usr_cnt"].to_numpy()[
                        0
                    ]
                    # NOTE update cumulative sum
                    cumulative_sum += iterate_sum

        return x_positions, y_positions

    def plot(self, as_data_graph=False):
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_name
        time_col = self.__eventstream.schema.event_timestamp
        event_index_col = self.__eventstream.schema.event_index

        def _rank(
            x: pd.Series,
        ) -> np.array:
            """
            It is a rank timeseries function for pandas row applying
            Parameters
            ----------
            x : pandas Series
                A pandas series row
            Returns
            -------
            rank : numpy array
                A rank of pandas series row
            """

            return x[time_col].rank(method="first").astype(int)

        def _get_next_event(x: pd.Series) -> np.array:
            """
            It is a next event function for pandas row applying

            Parameters
            ----------
            x : pandas Series
                A pandas series row

            Returns
            -------
            shifted event : numpy array
                A previous event of pandas series row
            """

            return x[event_col].shift(-1)

        def _get_time_diff(x: pd.Series) -> np.array:
            """
            It is a timeseries difference function for pandas row applying

            Parameters
            ----------
            x : pd.Series
                A pandas series row

            Returns
            -------
            timeseries difference : numpy array
                A difference between the current and the previous pandas-series row
            """

            return x[time_col].shift(-1) - x[time_col]

        # NOTE copy data
        data = self.__eventstream.to_dataframe().copy()

        # NOTE set new columns using declared functions
        data[time_col] = pd.to_datetime(data[time_col])
        data["rank_event"] = data.groupby(user_col)[event_index_col].rank(method="first").astype(int)

        # # NOTE ended
        # data_ended_full = pd.DataFrame()
        # for rank_event in data['rank_event'].unique():
        #
        #     if rank_event == 1:
        #         continue
        #
        #     user_id_ended = set(data.loc[(data['rank_event'] == rank_event - 1), user_col]) - \
        #                     set(data.loc[(data['rank_event'] == rank_event), user_col])
        #     # TODO: Убрать ended, перейти на синтетическое событие end из препроцессинга
        #     data_ended = data.loc[(data['rank_event'] == rank_event - 1) & (data[user_col].isin(user_id_ended))].copy()
        #     data_ended.loc[:, 'rank_event'] = rank_event
        #     data_ended.loc[:, event_col] = 'ended'
        #     data_ended.loc[:, time_col] += datetime.timedelta(milliseconds=1)
        #
        #     data_ended_inc = pd.DataFrame()
        #
        #     if rank_event > 2:
        #         data_ended_inc = data_ended_full[data_ended_full['rank_event'] == rank_event - 1].copy()
        #         data_ended_inc.loc[:, 'rank_event'] = rank_event
        #         data_ended_inc.loc[:, event_col] = 'ended'
        #
        #     data_ended_full = pd.concat(objs=[data_ended_full, data_ended_inc, data_ended])

        # data = pd.concat(objs=[data, data_ended_full])
        data = data.sort_values(by=["rank_event", time_col]).reset_index(drop=True)
        grouped = data.groupby(user_col)
        data.loc[:, "next_event"] = grouped.apply(lambda x: _get_next_event(x)).reset_index(0, drop=True)
        # NOTE set links between ended
        # data.loc[data[event_col] == 'ended', 'next_event'] = 'ended'
        data.loc[:, "time_to_next"] = grouped.apply(lambda x: _get_time_diff(x)).reset_index(0, drop=True)

        # NOTE threshold
        data.loc[:, "detail"] = data.groupby(by=["rank_event", event_col])[user_col].transform("nunique")
        data.loc[:, "total"] = data.loc[data["rank_event"] == 1, user_col].nunique()
        data.loc[:, "perc"] = data.loc[:, "detail"] / data.loc[:, "total"]

        if type(self.thresh) == int:
            data.loc[data["detail"] <= self.thresh, event_col] = "thresholded"

        else:
            data_thresh = data.loc[data.rank_event <= self.max_steps, :]
            thresh_events = data_thresh.groupby(by=[event_col])["perc"].max().reset_index()
            thresh_events = thresh_events[
                (thresh_events["perc"] <= self.thresh) & (~thresh_events[event_col].isin(["ended", self.target]))
            ][event_col]

            data.loc[data[event_col].isin(thresh_events), event_col] = f"thresholded_{len(thresh_events)}"

        if self.thresh > 0.0:
            # NOTE kinda hardcoded part in case of using threshold
            data.loc[:, "rank_event"] = grouped.apply(_rank).reset_index(0, drop=True)
            data.loc[:, "next_event"] = grouped.apply(lambda x: _get_next_event(x)).reset_index(0, drop=True)

            # NOTE set links between ended
            data.loc[data[event_col] == "ended", "next_event"] = "ended"
            data.loc[:, "time_to_next"] = grouped.apply(lambda x: _get_time_diff(x)).reset_index(0, drop=True)

        # NOTE skip mean calculating error
        data["time_to_next"].fillna(data["time_to_next"].min(), inplace=True)

        # NOTE use max_steps for filtering data
        data = data.loc[data.rank_event <= self.max_steps, :]
        # NOTE get all possible events
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
            data.groupby(by=["rank_event", event_col])[user_col]
            .nunique()
            .reset_index()
            .rename(columns={user_col: "usr_cnt"})
        )

        data_grp_nodes.loc[:, "usr_cnt_total"] = data_grp_nodes.groupby(by=["rank_event"])["usr_cnt"].transform("sum")

        data_grp_nodes.loc[:, "perc"] = round(
            (data_grp_nodes.loc[:, "usr_cnt"] / data_grp_nodes.loc[:, "usr_cnt_total"]) * 100, 2
        )

        data_grp_nodes.sort_values(
            by=["rank_event", "usr_cnt", event_col],
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

        # NOTE placing ended at the end
        data_grp_nodes.loc[data_grp_nodes[event_col] == "ended", "sorting"] = 101

        # NOTE using custom ordering
        data_grp_nodes.loc[:, "sorting"] = data_grp_nodes.loc[:, "sorting"].astype(int)

        # NOTE doing loop for valid ranking
        for rank_event in data_grp_nodes["rank_event"].unique():
            # NOTE saving last level order
            data_grp_nodes.loc[:, "order_by"] = (
                data_grp_nodes.groupby(by=[event_col])["index"].transform("shift").fillna(100).astype(int)
            )

            # NOTE placing ended events at the end
            data_grp_nodes.loc[data_grp_nodes[event_col] == "ended", "sorting"] = 101

            # NOTE creating new indexes
            data_grp_nodes.sort_values(
                by=["rank_event", "sorting", "order_by", "usr_cnt", event_col],
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

        for rank_event in data_grp_nodes["rank_event"].unique():
            data_for_plot["nodes_dict"].update({rank_event: dict()})
            _sources, _color, _sources_index, _percs = (
                data_grp_nodes.loc[data_grp_nodes["rank_event"] == rank_event, [event_col, "color", "index", "perc"]]
                .to_numpy()
                .T
            )

            data_for_plot["nodes_dict"][rank_event].update(
                {
                    "sources": list(_sources),
                    "color": list(_color),
                    "sources_index": list(_sources_index),
                    "percs": list(_percs),
                }
            )

        # NOTE create links aggregated dataframe
        data_grp_links = (
            data[data["rank_event"] <= self.max_steps - 1]
            .groupby(by=["rank_event", event_col, "next_event"])[[user_col, "time_to_next"]]
            .agg({user_col: ["count"], "time_to_next": ["sum"]})
            .reset_index()
            .rename(columns={user_col: "usr_cnt", "time_to_next": "time_to_next_sum"})
        )
        data_grp_links.columns = data_grp_links.columns.droplevel(1)

        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["rank_event", event_col, "index"]],
            how="inner",
            on=["rank_event", event_col],
        )

        data_grp_links.loc[:, "next_rank_event"] = data_grp_links["rank_event"] + 1

        data_grp_links = data_grp_links.merge(
            data_grp_nodes[["rank_event", event_col, "index"]].rename(
                columns={"rank_event": "next_rank_event", event_col: "next_event", "index": "next_index"}
            ),
            how="inner",
            on=["next_rank_event", "next_event"],
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

        if as_data_graph:
            return data, data_grp_nodes, data_grp_links
        else:
            return None, None, None
