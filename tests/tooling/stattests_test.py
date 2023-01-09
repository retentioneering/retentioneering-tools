import math

from src.tooling.stattests import StatTests
from tests.tooling.fixtures.stattests import continuous_data, cutted_data, simple_data
from tests.tooling.fixtures.stattests_corr import (
    chi2_contingency_corr,
    fisher_exact_corr,
    ks_2samp_corr,
    ztest_corr,
)


class TestStatTest:
    def test_stattest__ttest_p_val(self, simple_data):
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        st.fit()
        correct = 0.13545
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ttest_mean(self, simple_data):
        correct = 7.0
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        st.fit()
        result = st.values["group_one_mean"]
        assert math.isclose(result, correct, abs_tol=0.1)

    def test_stattest__ttest_greatest(self, simple_data):
        correct = True
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        st.fit()
        result = st.values["is_group_one_greatest"]
        assert correct == result

    def test_stattest__mannwhitneyu_p_val(self, simple_data):
        correct = 0.1859
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="mannwhitneyu",
        )
        st.fit()
        result = st.values["p_val"]

        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ks_2samp(self, continuous_data, ks_2samp_corr):
        correct = ks_2samp_corr
        st = StatTests(
            continuous_data,
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x["seconds"].mean(),
            group_names=("group_1", "group_2"),
            test="ks_2samp",
        )
        st.fit()
        result = st.values
        numeric_values = [
            "group_one_mean",
            "group_one_SD",
            "group_two_mean",
            "group_two_SD",
            "p_val",
            "power_estimated",
        ]
        for item in result:
            if item in numeric_values:
                result[item] = result[item].round(3)
        assert result == correct

    def test_stattest__chi2_contingency(self, simple_data, chi2_contingency_corr):
        correct = chi2_contingency_corr["p_val"]
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: "payment_done" in x["event"].values,
            group_names=("group_1", "group_2"),
            test="chi2_contingency",
        )
        st.fit()
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.1)

    def test_stattest__ztest(self, simple_data, ztest_corr):
        correct = ztest_corr["p_val"]
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ztest",
        )
        st.fit()
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__fisher_exact(self, cutted_data, fisher_exact_corr):
        correct = fisher_exact_corr["p_val"]
        st = cutted_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: "payment_done" in x["event"].values,
            group_names=("group_1", "group_2"),
            test="fisher_exact",
        )
        st.fit()
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ttest_alpha(self, simple_data):
        correct = 0.4390
        st = simple_data.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
            alpha=0.1,
        )
        st.fit()
        result = st.values["power_estimated"]
        assert math.isclose(result, correct, abs_tol=0.001)


class TestEventstreamStattests:
    def test_stattests_working(self, simple_data):
        source = simple_data
        try:
            source.stattests(
                groups=([1, 2, 3, 4], [5, 6, 7, 8]),
                func=lambda x: x.shape[0],
                group_names=("group_1", "group_2"),
                test="ttest",
            )
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.stattests. " + str(e))
        try:
            source.stattests(
                groups=([1, 2, 3, 4], [5, 6, 7, 8]),
                func=lambda x: x.shape[0],
                group_names=("group_1", "group_2"),
                test="chi2_contingency",
            )
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.stattests. " + str(e))
