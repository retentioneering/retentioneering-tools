# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from datetime import datetime

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import rcParams
import matplotlib.pylab as plt

from .plot_utils import __save_plot__


_ = pd.DataFrame()


@__save_plot__
def plot_projection(*,
                    projection,
                    targets,
                    legend_title):
    rcParams['figure.figsize'] = 8, 6

    scatter = sns.scatterplot(x=projection[:, 0],
                              y=projection[:, 1],
                              hue=targets,
                              legend='full',
                              palette=sns.color_palette("bright")[0:np.unique(targets).shape[0]])

    # move legend outside the box
    scatter.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.).set_title(legend_title)
    plt.setp(scatter.get_legend().get_title(), fontsize='12')

    plot_name = 'project_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = _.rete.retention_config['experiments_folder'] + '/' + plot_name

    return scatter, plot_name, projection, _.rete.retention_config