import math

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.stattests import StatTests
from tests.tooling.fixtures.stattests import (
    continuous_data,
    non_equal_target_data,
    simple_data,
)
from tests.tooling.fixtures.stattests_corr import (
    chi2_contingency_corr,
    fisher_exact_corr,
    ks_2samp_corr,
    ztest_corr,
)


class TestStatTest:
    def test_stattest__ttest_p_val(self, simple_data: EventstreamType) -> None:
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        correct = 0.13545
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ttest_mean(self, simple_data: EventstreamType) -> None:
        correct = 7.0
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        result = st.values["group_one_mean"]
        assert math.isclose(result, correct, abs_tol=0.1)

    def test_stattest__ttest_greatest(self, simple_data: EventstreamType) -> None:
        correct = False
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        result = st.values["is_group_one_greatest"]
        assert correct == result

    def test_stattest__mannwhitneyu_p_val(self, simple_data: EventstreamType) -> None:
        correct = 0.1859
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="mannwhitneyu",
        )
        result = st.values["p_val"]

        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ks_2samp(self, continuous_data: EventstreamType, ks_2samp_corr) -> None:
        correct = ks_2samp_corr
        st = StatTests(continuous_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x["seconds"].mean(),
            group_names=("group_1", "group_2"),
            test="ks_2samp",
        )
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

    def test_stattest__chi2_contingency(self, simple_data: EventstreamType, chi2_contingency_corr) -> None:
        correct = chi2_contingency_corr["p_val"]
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: "payment_done" in x["event"].values,
            group_names=("group_1", "group_2"),
            test="chi2_contingency",
        )
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.1)

    def test_stattest__ztest(self, simple_data: EventstreamType, ztest_corr) -> None:
        correct = ztest_corr["p_val"]
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ztest",
        )
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__fisher_exact(self, non_equal_target_data: EventstreamType, fisher_exact_corr) -> None:
        correct = fisher_exact_corr["p_val"]
        st = StatTests(eventstream=non_equal_target_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: "payment_done" in x["event"].values,
            group_names=("group_1", "group_2"),
            test="fisher_exact",
        )
        result = st.values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattest__ttest_alpha(self, simple_data: EventstreamType) -> None:
        correct = 0.4390
        st = StatTests(eventstream=simple_data)
        st.fit(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
            alpha=0.1,
        )
        result = st.values["power_estimated"]
        assert math.isclose(result, correct, abs_tol=0.001)


class TestEventstreamStattests:
    def test_stattests_working(self, simple_data: EventstreamType) -> None:
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
