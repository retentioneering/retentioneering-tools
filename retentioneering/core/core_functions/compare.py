# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from scipy.stats import ks_2samp, mannwhitneyu

from ...visualization import plot_compare
from statsmodels.stats.power import TTestIndPower
from statsmodels.stats.weightstats import ttest_ind
import numpy as np
import math

TESTS_LIST = ['ks_2samp', 'mannwhitneyu']


def compare(self, *,
            groups,
            function,
            test,
            group_names=('group_1', 'group_2'),
            alpha=0.05):
    """
    Tests selected metric between two groups of users.

    Parameters
    ----------
    groups: tuple (optional, default None)
        Must contain tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
        of user_id`s (list, tuple or set).
    function: function(x) -> number
        Selected metrics. Must contain a function wich takes as an argument dataset for
        single user trajectory and returns a single numerical value.
    group_names: tuple (optional, default: ('group_1', 'group_2'))
        Names for selected groups g_1 and g_2.
    test: {‘mannwhitneyu’, 'ttest', ‘ks_2samp’}
        Test the null hypothesis that 2 independent samples are drawn from the same
        distribution. One-sided tests are used, meaning that distributions are compared
        'less' or 'greater'. Rule of thumbs is: for discrete variables (like convertions
        or number of purchase) use Mann-Whitney (‘mannwhitneyu’) test or t-test (‘ttest’).
         For continious variables (like average_check) use Kolmogorov-Smirnov test ('ks_2samp').
    alpha: float (optional, default 0.05)
        Selected level of significance.

    Returns
    -------
    Prints statistical comparison between two groups over selected metric and test

    Plots a distribution for selected metrics for two groups

    """
    # obtain two populations for each group
    index_col = self.retention_config['user_col']
    data = self._obj
    g1 = data[data[index_col].isin(groups[0])].copy()
    g2 = data[data[index_col].isin(groups[1])].copy()

    # obtain two distributions:
    g1_data = g1.groupby(index_col).apply(function).dropna().values
    g2_data = g2.groupby(index_col).apply(function).dropna().values

    # plot graphs
    plot_compare.compare(num_data=(g1_data, g2_data),
                         group_names=group_names)

    # calculate test statistics
    if test:
        print(f"{group_names[0]} (mean \u00B1 SD): {g1_data.mean():.3f} \u00B1 {g1_data.std():.3f}, n = {len(g1_data)}")
        print(f"{group_names[1]} (mean \u00B1 SD): {g2_data.mean():.3f} \u00B1 {g2_data.std():.3f}, n = {len(g2_data)}")

        p_val_norm, power_norm = _stat_test(g1_data, g2_data, test, alpha)
        p_val_rev, power_rev = _stat_test(g2_data, g1_data, test, alpha)

        if p_val_norm < p_val_rev:
            p_val = p_val_norm
            power = power_norm
            label_max = group_names[0]
            label_min = group_names[1]
        else:
            p_val = p_val_rev
            power = power_rev
            label_max = group_names[1]
            label_min = group_names[0]

        print(f"'{label_max}' is greater than '{label_min}' with P-value: {p_val:.5f}")
        print(f"power of the test: {100*power:.2f}%")


def _stat_test(data_max, data_min, test, alpha):
    # calculate effect size
    if (max(data_max) <= 1 and
            min(data_max) >= 0 and
            max(data_min) <= 1 and
            min(data_min) >= 0):
        # if analyze proportions use Cohen's h:
        effect_size = _cohenh(data_max, data_min)
    else:
        # for other variables use Cohen's d:
        effect_size = _cohend(data_max, data_min)

    # calculate power
    power = TTestIndPower().power(effect_size=effect_size,
                                  nobs1=len(data_max),
                                  ratio=len(data_min) / len(data_max),
                                  alpha=alpha,
                                  alternative='larger')

    if test == 'ks_2samp':
        p_val = ks_2samp(data_max, data_min, alternative='less')[1]
    elif test == 'mannwhitneyu':
        p_val = mannwhitneyu(data_max, data_min, alternative='greater')[1]
    elif test == 'ttest':
        p_val = ttest_ind(data_max, data_min, alternative='larger')[1]
    return p_val, power


# function to calculate Cohen's d:
def _cohend(d1, d2):
    n1, n2 = len(d1), len(d2)
    s1, s2 = np.var(d1, ddof=1), np.var(d2, ddof=1)
    s = math.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
    u1, u2 = np.mean(d1), np.mean(d2)
    return (u1 - u2) / s


# function to calculate Cohen's h:
def _cohenh(d1, d2):
    u1, u2 = np.mean(d1), np.mean(d2)
    return 2*(math.asin(math.sqrt(u1)) -
              math.asin(math.sqrt(u2)))
