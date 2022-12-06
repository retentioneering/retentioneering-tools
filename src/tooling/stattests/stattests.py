from __future__ import annotations

import math
from typing import Callable, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
from scipy.stats import chi2_contingency, fisher_exact, ks_2samp, mannwhitneyu
from scipy.stats.contingency import crosstab
from statsmodels.stats.power import TTestIndPower
from statsmodels.stats.weightstats import ttest_ind, ztest

from src.eventstream.types import EventstreamType
from src.tooling.stattests.constants import TEST_NAMES


class StatTests:
    """
    Tests selected metric between two groups of users.

    Parameters
    ----------
    groups : tuple, optional
        Must contain tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
        of user_id`s (list, tuple or set).
    objective : function(x) -> number
        Selected metrics. Must contain a function which takes as an argument dataset for
        single user trajectory and returns a single numerical value.
    group_names : tuple (optional, default: ('group_1', 'group_2'))
        Names for selected groups g_1 and g_2.
    test : {‘mannwhitneyu’, 'ttest', 'ztest', ‘ks_2samp’, 'chi2_contingency', 'fisher_exact'}
        Test the null hypothesis that 2 independent samples are drawn from the same
        distribution. All tests present, except for 'chi2_contingency', are one-sided - meaning that
        distributions are compared 'less' or 'greater'. Rule of thumbs is: for discrete variables (like convertions
        or number of purchase) use Mann-Whitney (‘mannwhitneyu’) test or t-test (‘ttest’).
         For continious variables (like average_check) use Kolmogorov-Smirnov test ('ks_2samp').
    alpha : float (optional, default 0.05)
        Selected level of significance.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        test: TEST_NAMES,
        groups: Tuple[list[str | int], list[str | int]],
        objective: Callable = lambda x: x.shape[0],
        group_names: Tuple[str, str] = ("group_1", "group_2"),
        alpha: float = 0.05,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.groups = groups
        self.objective = objective
        self.test = test
        self.group_names = group_names
        self.alpha = alpha
        self.g1_data: list[str | int] = list()
        self.g2_data: list[str | int] = list()
        self.p_val, self.power, self.label_min, self.label_max = 0.0, 0.0, "", ""
        self.is_fitted = False

    def fit(self) -> None:
        """
        Computes specified test statistic, along with test result description

        """
        self.g1_data, self.g2_data = self._get_group_values()
        self.p_val, self.power, self.label_min, self.label_max = self._get_sorted_test_results()
        self.is_fitted = True

    def _get_group_values(self) -> Tuple[list, list]:
        data = self.__eventstream.to_dataframe()
        # obtain two populations for each group
        g1 = data[data[self.user_col].isin(self.groups[0])].copy()
        g2 = data[data[self.user_col].isin(self.groups[1])].copy()

        # obtain two distributions:
        g1_data = list(g1.groupby(self.user_col).apply(self.objective).dropna().astype(float).values)
        g2_data = list(g2.groupby(self.user_col).apply(self.objective).dropna().astype(float).values)
        return g1_data, g2_data

    def _get_test_results(self, data_max: list, data_min: list) -> Tuple[float, float]:
        # calculate effect size
        if max(data_max) <= 1 and min(data_max) >= 0 and max(data_min) <= 1 and min(data_min) >= 0:
            # if analyze proportions use Cohen's h:
            effect_size = self._cohenh(data_max, data_min)
        else:
            # for other variables use Cohen's d:
            effect_size = self._cohend(data_max, data_min)

        # calculate power
        power = TTestIndPower().power(
            effect_size=effect_size,
            nobs1=len(data_max),
            ratio=len(data_min) / len(data_max),
            alpha=self.alpha,
            alternative="larger",
        )

        if self.test == "ks_2samp":
            p_val = ks_2samp(data_max, data_min, alternative="less")[1]
        elif self.test == "mannwhitneyu":
            p_val = mannwhitneyu(data_max, data_min, alternative="greater")[1]
        elif self.test == "ttest":
            p_val = ttest_ind(data_max, data_min, alternative="larger")[1]
        elif self.test == "ztest":
            p_val = ztest(data_max, data_min, alternative="larger")[1]
        elif self.test == "chi2_contingency":
            freq_table = self._get_freq_table(data_max, data_min)
            p_val = chi2_contingency(freq_table)[1]
        elif self.test == "fisher_exact":
            freq_table = self._get_freq_table(data_max, data_min)
            p_val = fisher_exact(freq_table, alternative="greater")[1]
        else:
            raise ValueError("The argument test is not supported. Supported tests are: {}".format(*TEST_NAMES))

        return p_val, power

    def _get_freq_table(self, a: list, b: list) -> list:
        labels = ["A"] * len(a) + ["B"] * len(b)
        values = np.concatenate([a, b])
        return crosstab(labels, values)[1]

    def _get_sorted_test_results(self) -> Tuple[float, float, str, str]:
        p_val_norm, power_norm = self._get_test_results(self.g1_data, self.g2_data)
        p_val_rev, power_rev = self._get_test_results(self.g2_data, self.g1_data)
        if p_val_norm < p_val_rev:
            p_val = p_val_norm
            power = power_norm
            label_max = self.group_names[0]
            label_min = self.group_names[1]
        else:
            p_val = p_val_rev
            power = power_rev
            label_max = self.group_names[1]
            label_min = self.group_names[0]
        return p_val, power, label_max, label_min

    # function to calculate Cohen's d:
    def _cohend(self, d1: list, d2: list) -> float:
        n1, n2 = len(d1), len(d2)
        s1, s2 = np.var(d1, ddof=1), np.var(d2, ddof=1)
        s = float(math.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2)))
        u1, u2 = float(np.mean(d1)), float(np.mean(d2))
        return (u1 - u2) / s

    # function to calculate Cohen's h:
    def _cohenh(self, d1: list, d2: list) -> float:
        u1, u2 = np.mean(d1), np.mean(d2)
        return 2 * (math.asin(math.sqrt(u1)) - math.asin(math.sqrt(u2)))

    def plot(self) -> Tuple[go.Figure, str]:
        """
        Plots with distribution for selected metrics for two groups.

        Returns
        -------
        go.Figure
        """
        data1 = pd.DataFrame(data={"data": self.g1_data, "groups": self.group_names[0]})
        data2 = pd.DataFrame(data={"data": self.g2_data, "groups": self.group_names[1]})
        combined_stats = pd.concat([data1, data2]).reset_index()
        compare_plot = sns.displot(data=combined_stats, x="data", hue="groups", multiple="dodge")
        compare_plot.set(xlabel=None)
        return compare_plot

    def values(
        self,
    ) -> dict:
        """
        Results of statistical comparison between two groups over selected metric and test.

        Returns
        -------
        dict

        """
        assert self.is_fitted
        res_dict = {
            "group_one_name": "group_1",
            "group_one_size": 0,
            "group_one_mean": 0.0,
            "group_one_SD": 0.0,
            "group_two_name": "group_2",
            "group_two_size": 0,
            "group_two_mean": 0.0,
            "group_two_SD": 0.0,
            "greatest_group_name": "group_1",
            "is_group_one_greatest": True,
            "p_val": 0.0,
            "power_estimated": 0.0,
        }
        res_dict["group_one_name"], res_dict["group_one_size"] = self.group_names[0], len(self.g1_data)
        res_dict["group_one_mean"], res_dict["group_one_SD"] = (
            np.array(self.g1_data).mean(),
            np.array(self.g1_data).std(),
        )
        res_dict["group_two_name"], res_dict["group_two_size"] = self.group_names[1], len(self.g2_data)
        res_dict["group_two_mean"], res_dict["group_two_SD"] = (
            np.array(self.g2_data).mean(),
            np.array(self.g2_data).std(),
        )
        res_dict["greatest_group_name"] = self.label_max
        res_dict["is_group_one_greatest"] = self.label_max == self.group_names[0]
        res_dict["p_val"] = self.p_val
        if self.test in ["ztest", "ttest", "mannwhitneyu", "ks_2samp"]:
            res_dict["power_estimated"] = self.power
        return res_dict
