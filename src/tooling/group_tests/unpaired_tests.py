import math

import numpy as np
from scipy.stats import chi2_contingency, fisher_exact, ks_2samp, mannwhitneyu
from scipy.stats.contingency import crosstab
from statsmodels.stats.power import TTestIndPower
from statsmodels.stats.weightstats import ttest_ind, ztest

from src.tooling.group_tests.test_plots import plot_test_groups


class UnpairedGroupTest:
    """
    Tests selected metric between two groups of users.
    Parameters
    ----------
    groups: tuple (optional, default None)
        Must contain tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
        of user_id`s (list, tuple or set).
    function: function(x) -> number
        Selected metrics. Must contain a function which takes as an argument dataset for
        single user trajectory and returns a single numerical value.
    group_names: tuple (optional, default: ('group_1', 'group_2'))
        Names for selected groups g_1 and g_2.
    test: {‘mannwhitneyu’, 'ttest', 'ztest', ‘ks_2samp’, 'chi2_contingency', 'fisher_exact'}
        Test the null hypothesis that 2 independent samples are drawn from the same
        distribution. One-sided tests are used, meaning that distributions are compared
        'less' or 'greater'. Rule of thumbs is: for discrete variables (like convertions
        or number of purchase) use Mann-Whitney (‘mannwhitneyu’) test or t-test (‘ttest’).
         For continious variables (like average_check) use Kolmogorov-Smirnov test ('ks_2samp').
    alpha: float (optional, default 0.05)
        Selected level of significance.
    Methods
    -------
    print_test_results: Prints statistical comparison between two groups over selected metric and test
    plot_groups: Returns plots with distribution for selected metrics for two groups
    """

    def __init__(self, eventstream, groups, function, test, group_names=("group_1", "group_2"), alpha=0.05) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        # self.data = self.__eventstream.to_dataframe()
        self.groups = groups
        self.function = function
        self.test = test
        self.group_names = group_names
        self.alpha = alpha
        self.g1_data, self.g2_data = self._get_group_values()
        self.p_val, self.power, self.label_min, self.label_max = self._get_sorted_test_results()

    def _get_group_values(self):
        data = self.__eventstream.to_dataframe()
        # obtain two populations for each group
        g1 = data[data[self.user_col].isin(self.groups[0])].copy()
        g2 = data[data[self.user_col].isin(self.groups[1])].copy()

        # obtain two distributions:
        g1_data = g1.groupby(self.user_col).apply(self.function).dropna().astype(float).values
        g2_data = g2.groupby(self.user_col).apply(self.function).dropna().astype(float).values
        return g1_data, g2_data

    def _get_test_results(self, data_max, data_min):
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
            p_val = ttest_ind(data_max, data_min, alternative="larger")[1]
        elif self.test == "chi2_contingency":
            freq_table = self._get_freq_table(data_max, data_min)
            p_val = chi2_contingency(freq_table)[1]
        elif self.test == "fisher_exact":
            freq_table = self._get_freq_table(data_max, data_min)
            p_val = fisher_exact(freq_table, alternative="greater")[1]
        return p_val, power

    def _get_freq_table(self, a, b):
        labels = ["A" for i in a] + ["B" for i in b]
        values = np.concatenate([a, b])
        return crosstab(labels, values)[1]

    def _get_sorted_test_results(self):
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
    def _cohend(self, d1, d2):
        n1, n2 = len(d1), len(d2)
        s1, s2 = np.var(d1, ddof=1), np.var(d2, ddof=1)
        s = math.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
        u1, u2 = np.mean(d1), np.mean(d2)
        return (u1 - u2) / s

    # function to calculate Cohen's h:
    def _cohenh(self, d1, d2):
        u1, u2 = np.mean(d1), np.mean(d2)
        return 2 * (math.asin(math.sqrt(u1)) - math.asin(math.sqrt(u2)))

    def plot_groups(self):
        return plot_test_groups(num_data=(self.g1_data, self.g2_data), group_names=self.group_names)

    def get_test_results(self):
        res_dict = dict()
        res_dict['group_one_name'], res_dict['group_one_size'] = self.group_names[0], len(self.g1_data)
        res_dict['group_one_mean'], res_dict['group_one_SD'] = self.g1_data.mean(), self.g1_data.std()
        res_dict['group_two_name'], res_dict['group_two_size'] = self.group_names[1], len(self.g2_data)
        res_dict['group_two_mean'], res_dict['group_two_SD'] = self.g2_data.mean(), self.g2_data.std()
        res_dict['greatest_group_name'] = self.label_max
        res_dict['is_group_one_greatest'] = self.label_max == self.group_names[0]
        res_dict['p_val'] = self.p_val
        if self.test in ["ztest", "ttest", "mannwhitneyu", "ks_2samp"]:
            res_dict['power_estimated'] = self.power
        return res_dict
