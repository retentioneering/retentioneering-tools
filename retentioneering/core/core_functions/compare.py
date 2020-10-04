# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive, Non-Commercial Use License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from scipy.stats import ks_2samp, mannwhitneyu

from ...visualization import plot_compare

TESTS_LIST = ['ks_2samp', 'mannwhitneyu']

def compare(self, *,
            groups,
            function,
            test,
            group_names=('group_1', 'group_2')):
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
    test: {‘ks_2samp’, ‘mannwhitneyu’}
        Test the null hypothesis that 2 independent samples are drawn from the same
        distribution. One-sided tests are used, meaning that distributions are compared
        'less' or 'greater'. For discrete variables (like convertions or number of purchase)
        use Mann-Whitney test (‘mannwhitneyu’). For continious variables (like average_check)
        use Kolmogorov-Smirnov test ('ks_2samp').

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
        test_func = globals()[test]
        print(f"{group_names[0]} (mean \u00B1 SD): {g1_data.mean():.3f} \u00B1 {g1_data.std():.3f}, n = {len(g1_data)}")
        print(f"{group_names[1]} (mean \u00B1 SD): {g2_data.mean():.3f} \u00B1 {g2_data.std():.3f}, n = {len(g2_data)}")

        if test == 'ks_2samp':
            p_less = (test_func(g1_data, g2_data, alternative='less')[1])
            p_greater = (test_func(g1_data, g2_data, alternative='greater')[1])
        elif test == 'mannwhitneyu':
            p_greater = (test_func(g1_data, g2_data, alternative='less')[1])
            p_less = (test_func(g1_data, g2_data, alternative='greater')[1])

        if p_less < p_greater:
            print(f"'{group_names[0]}' is greater than '{group_names[1]}' with P-value: {p_less:.5f}")
        else:
            print(f"'{group_names[0]}' is less than '{group_names[1]}' with P-value: {p_greater:.5f}")
