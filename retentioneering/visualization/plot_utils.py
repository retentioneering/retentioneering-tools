# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import json
from datetime import datetime
import matplotlib.pyplot as plt

import seaborn as sns
from functools import wraps
from IPython.display import IFrame, display, HTML


def __save_plot__(func):
    @wraps(func)
    def save_plot_wrapper(*args, **kwargs):
        sns.mpl.pyplot.show()
        sns.mpl.pyplot.close()
        res = func(*args, **kwargs)
        if len(res) == 2:
            (vis_object, name), res, cfg = res, None, None
        elif len(res) == 3:
            (vis_object, name, res), cfg = res, None
        else:
            vis_object, name, res, cfg = res
        idx = 'id: ' + str(int(datetime.now().timestamp()))
        coords = vis_object.axis()

        vis_object.get_figure().savefig(name, bbox_inches="tight", dpi=cfg.get('save_dpi') or 200)
        return res

    return save_plot_wrapper


class ___FigureWrapper__(object):
    def __init__(self, fig):
        self.fig = fig

    def get_figure(self):
        return self.fig

    def axis(self):
        if len(self.fig.axes) > 1:
            x = self.fig.axes[1].axis()
        else:
            x = self.fig.axes[0].axis()
        return (x[0] / 64, x[0] + (x[1] - x[0]) / 50, x[2] / 1.5, x[3] / 1.5)

    def text(self, *args, **kwargs):
        self.fig.text(*args, **kwargs)


class ___DynamicFigureWrapper__(object):
    def __init__(self, fig, interactive, width, height, links):
        self.fig = fig
        self.interactive, self.width, self.height = interactive, width, height
        self.links = links

    def get_figure(self):
        savefig = __SaveFigWrapper__(self.fig, self.interactive, self.width, self.height)
        return savefig

    def text(self, x, y, text, *args, **kwargs):
        # parts = self.fig.split('<main>')
        # res = parts[:1] + [f'<p>{text}</p>'] + parts[1:]
        # self.fig = '\n'.join(res)
        pass

    def get_raw(self, path):
        base = '.'.join(path.split('.')[:-1])
        with open(base + '_config.json', 'w', encoding="utf-8") as f:
            json.dump(self.links, f)
        return base + '_config.json'

    @staticmethod
    def axis():
        return 4 * [0]


class __SaveFigWrapper__(object):
    def __init__(self, data, interactive=True, width=1000, height=700):
        self.data = data
        self.interactive = interactive
        self.width = width
        self.height = height

    def savefig(self, name, **kwargs):
        with open(name, 'w', encoding="utf-8") as f:
            f.write(self.data)
        if self.interactive:
            display(IFrame(name, width=self.width + 200, height=self.height + 200))
