# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import plotly.graph_objects as go
from datetime import datetime
import pandas as pd
_ = pd.DataFrame()


def plot_stacked_funnel(groups):
    data = []

    for t in groups.keys():
        trace = go.Funnel(
            name=t,
            y=groups[t]['targets'],
            x=groups[t]['values'],
            textinfo="value+percent initial+percent previous"
        )
        data.append(trace)

    layout = go.Layout(margin={"l": 180, "r": 0, "t": 30, "b": 0, "pad": 0},
                       funnelmode="stack",
                       showlegend=True,
                       hovermode='closest',
                       legend=dict(orientation="v",
                                   bgcolor='#E2E2E2',
                                   xanchor='left',
                                   font=dict(
                                       size=12)
                                   )
                       )

    fig = go.Figure(data, layout)

    plot_name = 'funnel_plot_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.html'
    path = _.rete.retention_config['experiments_folder'] + '/' + plot_name
    fig.write_html(path)

    return fig
