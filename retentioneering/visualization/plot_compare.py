# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from datetime import datetime
import seaborn as sns
import pandas as pd

from .plot_utils import __save_plot__

_ = pd.DataFrame()

def compare(*,
            num_data,
            group_names):

    data1 = pd.DataFrame(data={'data': num_data[0], 'groups': group_names[0]})
    data2 = pd.DataFrame(data={'data': num_data[1], 'groups': group_names[1]})
    combined_stats = pd.concat([data1, data2])

    compare_plot = sns.displot(data=combined_stats, x="data", hue="groups", multiple="dodge")

    compare_plot.set(xlabel=None)

    # move legend outside the box
    #compare_plot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

    plot_name = 'compare_plot_{}'.format(datetime.now()).replace(':', '_').replace('.', '_') + '.svg'
    plot_name = _.rete.retention_config['experiments_folder'] + '/' + plot_name
    return compare_plot, plot_name, None, _.rete.retention_config