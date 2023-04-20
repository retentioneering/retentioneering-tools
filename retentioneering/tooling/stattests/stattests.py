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

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.stattests.constants import STATTEST_NAMES


def _cohend(sample1: list, sample2: list) -> float:
    n1, n2 = len(sample1), len(sample2)
    s1, s2 = np.var(sample1, ddof=1), np.var(sample2, ddof=1)
    s = float(math.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2)))
    mean_sample1, mean_sample2 = float(np.mean(sample1)), float(np.mean(sample2))
    return (mean_sample1 - mean_sample2) / s


def _cohenh(sample1: list, sample2: list) -> float:
    mean_sample1, mean_sample2 = np.mean(sample1), np.mean(sample2)
    return 2 * (math.asin(math.sqrt(mean_sample1)) - math.asin(math.sqrt(mean_sample2)))


class StatTests:
    """
    A class for determining statistical difference between two groups of users.

    Parameters
    ----------
    eventstream : EventstreamType

    See Also
    --------
    .Eventstream.stattests : Call StatTests tool as an eventstream method.

    Notes
    -----
    See :doc:`StatTests user guide</user_guides/stattests>` for the details.

    """

    __eventstream: EventstreamType
    test: STATTEST_NAMES
    groups: Tuple[list[str | int], list[str | int]]
    func: Callable
    group_names: Tuple[str, str]
    alpha: float
    g1_data: list[str | int]
    g2_data: list[str | int]

    output_template_numerical = "{0} (mean ± SD): {1:.3f} ± {2:.3f}, n = {3}"
    output_template_categorical = "{0} (size): n = {1}"
    p_val, power, label_min, label_max = 0.0, 0.0, "", ""
    is_fitted: bool

    def __init__(self, eventstream: EventstreamType) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp

        self.g1_data = list()
        self.g2_data = list()
        self.is_fitted = False

    def _get_group_values(self) -> Tuple[list, list]:
        data = self.__eventstream.to_dataframe()
        # obtain two populations for each group
        g1 = data[data[self.user_col].isin(self.groups[0])].copy()
        g2 = data[data[self.user_col].isin(self.groups[1])].copy()

        # obtain two distributions:
        if self.test not in ["chi2_contingency", "fisher_exact"]:
            g1_data = list(g1.groupby(self.user_col).apply(self.func).dropna().astype(float).values)
            g2_data = list(g2.groupby(self.user_col).apply(self.func).dropna().astype(float).values)
        else:
            g1_data = list(g1.groupby(self.user_col).apply(self.func).dropna().values)
            g2_data = list(g2.groupby(self.user_col).apply(self.func).dropna().values)
        return g1_data, g2_data

    def _get_freq_table(self, a: list, b: list) -> list:
        labels = ["A"] * len(a) + ["B"] * len(b)
        values = np.concatenate([a, b])
        if self.test == "fisher_exact" and np.unique(values).shape[0] != 2:
            raise ValueError("For Fisher exact test, there should be exactly 2 categories of observations in the data")
        return crosstab(labels, values)[1]

    def _get_test_results(self, data_max: list, data_min: list) -> Tuple[float, float]:
        if self.test in ["ztest", "ttest", "mannwhitneyu", "ks_2samp"]:
            # calculate effect size
            if max(data_max) <= 1 and min(data_max) >= 0 and max(data_min) <= 1 and min(data_min) >= 0:
                # if analyze proportions use Cohen's h:
                effect_size = _cohenh(data_max, data_min)
            else:
                # for other variables use Cohen's d:
                effect_size = _cohend(data_max, data_min)

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
        elif self.test in ["chi2_contingency", "fisher_exact"]:
            power = None
            if self.test == "chi2_contingency":
                freq_table = self._get_freq_table(data_max, data_min)
                p_val = chi2_contingency(freq_table)[1]
            elif self.test == "fisher_exact":
                freq_table = self._get_freq_table(data_max, data_min)
                p_val = fisher_exact(freq_table, alternative="greater")[1]
        else:
            raise ValueError(f"The argument test is not supported. Supported tests are: {STATTEST_NAMES}")
        return p_val, power  # type: ignore

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

    def fit(
        self,
        test: STATTEST_NAMES,
        groups: Tuple[list[str | int], list[str | int]],
        func: Callable,
        group_names: Tuple[str, str] = ("group_1", "group_2"),
        alpha: float = 0.05,
    ) -> None:
        """
        Calculates the stattests internal values with the defined parameters.
        Applying ``fit`` method is necessary for the following usage
        of any visualization or descriptive ``StatTests`` methods.

        Parameters
        ----------
        test : {'mannwhitneyu', 'ttest', 'ztest', 'ks_2samp', 'chi2_contingency', 'fisher_exact'}
            Test the null hypothesis that 2 independent samples are drawn from the same
            distribution. Supported tests are:

            - ``mannwhitneyu`` see :mannwhitneyu:`scipy documentation<>`.
            - ``ttest`` see :statsmodel_ttest:`statsmodels documentation<>`.
            - ``ztest`` see :statsmodel_ztest:`statsmodels documentation<>`.
            - ``ks_2samp`` see :scipy_ks:`scipy documentation<>`.
            - ``chi2_contingency`` see :scipy_chi2:`scipy documentation<>`.
            - ``fisher_exact`` see :scipy_fisher:`scipy documentation<>`.

        groups : tuple of list
            Must contain a tuple of two elements (g_1, g_2): g_1 and g_2 are collections
            of user_id`s.
        func : Callable
            Selected metrics. Must contain a function that takes a dataset as an argument for
            a single user trajectory and returns a single numerical value.
        group_names : tuple, default ('group_1', 'group_2')
            Names for selected groups g_1 and g_2.
        alpha : float, default 0.05
            Selected level of significance.

        """
        self.groups = groups
        self.func = func
        self.test = test
        self.group_names = group_names
        self.alpha = alpha

        self.g1_data, self.g2_data = self._get_group_values()
        self.p_val, self.power, self.label_min, self.label_max = self._get_sorted_test_results()
        self.is_fitted = True

    def plot(self) -> Tuple[go.Figure, str]:
        """
        Plots a barplot comparing the metric values between two groups.
        Should be used after :py:func:`fit`.

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

    @property
    def values(self) -> dict:
        """
        Returns the comprehensive results of the comparison between the two groups.
        Should be used after :py:func:`fit`.

        Returns
        -------
        dict

        """
        if not self.is_fitted:
            raise ValueError("The StatTests instance needs to be fitted before returning values")
        if self.test in ["ztest", "ttest", "mannwhitneyu", "ks_2samp"]:
            res_dict = {
                "group_one_name": self.group_names[0],
                "group_one_size": len(self.g1_data),
                "group_one_mean": np.array(self.g1_data).mean(),
                "group_one_SD": np.array(self.g1_data).std(),
                "group_two_name": self.group_names[1],
                "group_two_size": len(self.g2_data),
                "group_two_mean": np.array(self.g2_data).mean(),
                "group_two_SD": np.array(self.g2_data).std(),
                "greatest_group_name": self.label_max,
                "least_group_name": self.label_min,
                "is_group_one_greatest": self.label_max == self.group_names[0],
                "p_val": self.p_val,
                "power_estimated": self.power,
            }
        elif self.test in ["chi2_contingency", "fisher_exact"]:
            res_dict = {
                "group_one_name": self.group_names[0],
                "group_one_size": len(self.g1_data),
                "group_two_name": self.group_names[1],
                "group_two_size": len(self.g2_data),
                "p_val": self.p_val,
            }
        else:
            raise ValueError("Wrong test passed")
        return res_dict

    def display_results(self) -> None:
        if not self.is_fitted:
            raise ValueError("The StatTests instance needs to be fitted before displaying results")
        values = self.values
        if self.test in ["ztest", "ttest", "mannwhitneyu", "ks_2samp"]:
            print(
                self.output_template_numerical.format(
                    values["group_one_name"], values["group_one_mean"], values["group_one_SD"], values["group_one_size"]
                )
            )
            print(
                self.output_template_numerical.format(
                    values["group_two_name"], values["group_two_mean"], values["group_two_SD"], values["group_two_size"]
                )
            )
            print(
                "'{0}' is greater than '{1}' with p-value: {2:.5f}".format(
                    values["greatest_group_name"], values["least_group_name"], values["p_val"]
                )
            )
            print("power of the test: {0:.2f}%".format(100 * values["power_estimated"]))
        elif self.test in ["chi2_contingency", "fisher_exact"]:
            print(self.output_template_categorical.format(values["group_one_name"], values["group_one_size"]))
            print(self.output_template_categorical.format(values["group_two_name"], values["group_two_size"]))
            print("Group difference test with p-value: {:.5f}".format(values["p_val"]))
        else:
            raise ValueError("Wrong test passed")

    @property
    def params(self) -> dict:
        """
        Returns the parameters used for the last fitting.
        Should be used after :py:func:`fit`.

        """
        return {
            "test": self.test,
            "groups": self.groups,
            "func": self.func,
            "group_names": self.group_names,
            "alpha": self.alpha,
        }
