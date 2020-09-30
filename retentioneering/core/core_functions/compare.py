from scipy.stats import ks_2samp

from ...visualization import plot_compare


def compare(self, *,
            groups,
            function,
            group_names=('group_1', 'group_2'),
            test='ks_2samp'):
    """

    Parameters
    ----------
    groups
    function
    group_names
    test

    Returns
    -------

    """
    # obtain two populations for each group
    index_col = self.retention_config['index_col']
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

        p_less = (test_func(g1_data, g2_data, alternative='less')[1])
        p_greater = (test_func(g1_data, g2_data, alternative='greater')[1])

        if p_less < p_greater:
            print(f"'{group_names[0]}' is greater than '{group_names[1]}' with P-value: {p_less:.5f}")
        else:
            print(f"'{group_names[0]}' is less than '{group_names[1]}' with P-value: {p_greater:.5f}")
