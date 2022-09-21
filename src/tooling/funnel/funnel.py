from __future__ import annotations

from typing import Any, Sized

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.eventstream.types import EventstreamType


class Funnel:
    __eventstream: EventstreamType
    __default_layout = dict(
        margin={"l": 180, "r": 0, "t": 30, "b": 0, "pad": 0},
        funnelmode="stack",
        showlegend=True,
        hovermode="closest",
        legend=dict(orientation="v", bgcolor="#E2E2E2", xanchor="left", font=dict(size=12)),
    )
    groups: pd.Series | np.ndarray | list[list[int]]

    def __init__(
        self,
        eventstream: EventstreamType,
        targets: list[str],
        groups: pd.Series | np.ndarray | list[int] | Sized | None = None,
        group_names: list[str] | None = None,
    ) -> None:
        self.__eventstream = eventstream
        self.data = self.__eventstream.to_dataframe()
        self.index_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name

        self.targets = targets

        if groups is None:
            groups = [self.data[self.index_col].unique()]
            group_names = ["all users"]

        # IDK why but pyright thinks this is Funnel!!!
        self.groups = groups  # type: ignore

        if group_names is None:
            group_names = [f"group {i=}" for i in range(len(self.groups))]
        self.group_names = group_names

    def draw_plot(self) -> go.Figure:
        plot_params = self._calculate(targets=self.targets, groups=self.groups, group_names=self.group_names)
        data = self._calculate_plot_data(plot_params=plot_params)
        plot = self._plot_stacked_funnel(data=data)
        return plot

    def _calculate(
        self,
        targets: list[str],
        groups: pd.Series | np.ndarray | list[list[int]],
        group_names: list[str],
    ) -> dict[str, Any]:

        for idx, target in enumerate(targets):
            if type(target) is not list:
                targets[idx] = [target]  # type: ignore

        target_names = []
        for t in targets:
            # get name
            target_names.append(" | ".join(t).strip(" | "))
        res_dict = {}
        for group, group_name in zip(groups, group_names):
            # isolate users from group
            group_data = self.data[self.data[self.index_col].isin(group)]
            vals = [group_data[group_data[self.event_col].isin(target)][self.index_col].nunique() for target in targets]
            res_dict[group_name] = {"targets": target_names, "values": vals}

        return res_dict

    def _plot_stacked_funnel(self, data) -> go.Figure:
        layout = go.Layout(**self.__default_layout)
        fig = go.Figure(data, layout)

        # @TODO: Why do we need to write graph to html?
        # plot_name = 'funnel_plot_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.html'
        # path = f'/home/vladimir/Workspace/retentioneering/retentioneering-tools-new-arch/src/expetiments/{plot_name}'
        # _tmpfile = tempfile.NamedTemporaryFile()
        # path = _tmpfile.name
        # fig.write_html(path)

        return fig

    def _calculate_plot_data(self, plot_params: dict[str, Any]) -> list[go.Funnel]:
        data = []
        for t in plot_params.keys():
            trace = go.Funnel(
                name=t,
                y=plot_params[t]["targets"],
                x=plot_params[t]["values"],
                textinfo="value+percent initial+percent previous",
            )
            data.append(trace)

        return data
