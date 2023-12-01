import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.sequences import Sequences
from tests.tooling.fixtures.sequences_corr import (
    sequences_basic_corr,
    sequences_group_names_corr,
    sequences_groups_corr,
    sequences_heatmap_groups_corr,
    sequences_metrics_corr,
    sequences_precision_corr,
    sequences_sample_heatmap_corr,
    sequences_sample_size_none_corr,
    sequences_sample_size_two_corr,
    sequences_sequence_type_corr,
    sequences_sorting_corr,
    sequences_sorting_groups_corr,
    sequences_space_name_corr,
    sequences_threshold_corr,
    sequences_weight_col_corr,
)
from tests.tooling.fixtures.sequences_input import (
    test_stream_input,
    test_stream_input_space_name,
)

FLOAT_PRECISION = 2


def run_test(stream: EventstreamType, corr_fixture: pd.DataFrame, fit_params: dict, plot_params: dict = None) -> bool:
    seq = Sequences(eventstream=stream)
    seq.fit(**fit_params)

    if "weight_col" in fit_params:
        weight_col = fit_params["weight_col"]
    else:
        weight_col = "user_id"
    if plot_params:
        seq.plot(**plot_params)
    result = seq.values
    if not plot_params:
        result = result.round(FLOAT_PRECISION)
    return result.compare(corr_fixture).shape == (0, 0)


def run_test_sample_size(
    stream: EventstreamType, corr_fixture: pd.DataFrame, fit_params: dict, plot_params: dict = None
) -> (bool, bool):
    seq = Sequences(eventstream=stream)
    seq.fit(**fit_params)
    all_samples = seq.values["user_id_sample"].copy()
    seq.plot(**plot_params)

    result = seq.values.join(all_samples, rsuffix="_all")
    columns = ["user_id", "user_id_share", "count", "count_share", "sequence_type"]
    check = result.apply(lambda x: set(x.user_id_sample) <= set(x.user_id_sample_all), axis=1).sum()

    return (result[columns].compare(corr_fixture[columns]).shape == (0, 0), check == len(result))


class TestSequencesFit:
    def test_sequences__ngram_range(
        self, test_stream_input: EventstreamType, sequences_basic_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        assert run_test(test_stream_input, sequences_basic_corr, fit_params=fit_params)

    def test_sequences__groups(self, test_stream_input: EventstreamType, sequences_groups_corr: pd.DataFrame) -> None:
        fit_params = {"ngram_range": (2, 2), "groups": (["user1", "user2"], ["user3"])}
        assert run_test(test_stream_input, sequences_groups_corr, fit_params=fit_params)

    def test_sequences__group_names(
        self, test_stream_input: EventstreamType, sequences_group_names_corr: pd.DataFrame
    ) -> None:
        fit_params = {
            "ngram_range": (2, 2),
            "groups": (["user1", "user2"], ["user3"]),
            "group_names": ("pay", "no_pay"),
        }
        assert run_test(test_stream_input, sequences_group_names_corr, fit_params=fit_params)

    def test_sequences__weight_col(
        self, test_stream_input: EventstreamType, sequences_weight_col_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (2, 2), "weight_col": "session_id"}
        assert run_test(test_stream_input, sequences_weight_col_corr, fit_params=fit_params)

    def test_sequences__sequence_type(
        self, test_stream_input: EventstreamType, sequences_sequence_type_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (3, 3)}

        assert run_test(test_stream_input, sequences_sequence_type_corr, fit_params=fit_params)

    def test_sequences__space_name(
        self, test_stream_input_space_name: EventstreamType, sequences_space_name_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        assert run_test(test_stream_input_space_name, sequences_space_name_corr, fit_params=fit_params)


class TestSequencesPlot:
    def test_sequences__metrics(self, test_stream_input: EventstreamType, sequences_metrics_corr: pd.DataFrame) -> None:
        fit_params = {"ngram_range": (1, 2)}
        plot_params = {"metrics": "count", "sample_size": None}

        assert run_test(test_stream_input, sequences_metrics_corr, fit_params=fit_params, plot_params=plot_params)

    def test_sequences__threshold(
        self, test_stream_input: EventstreamType, sequences_threshold_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        plot_params = {"threshold": ("count", 2), "sample_size": None}
        assert run_test(test_stream_input, sequences_threshold_corr, fit_params=fit_params, plot_params=plot_params)

    def test_sequences__sorting(self, test_stream_input: EventstreamType, sequences_sorting_corr: pd.DataFrame) -> None:
        fit_params = {"ngram_range": (2, 2)}
        plot_params = {"sorting": ("count", True), "sample_size": None}
        assert run_test(test_stream_input, sequences_sorting_corr, fit_params=fit_params, plot_params=plot_params)

    def test_sequences__sorting_groups(
        self, test_stream_input: EventstreamType, sequences_sorting_groups_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (2, 2), "groups": (["user1", "user2"], ["user3"])}

        plot_params = {"sorting": (("user_id", "group_1"), True), "sample_size": None}
        assert run_test(
            test_stream_input, sequences_sorting_groups_corr, fit_params=fit_params, plot_params=plot_params
        )

    def test_sequences__sample_size_none(
        self, test_stream_input: EventstreamType, sequences_sample_size_none_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        plot_params = {"sample_size": None}
        assert run_test(
            test_stream_input, sequences_sample_size_none_corr, fit_params=fit_params, plot_params=plot_params
        )

    def test_sequences__sample_size_two(
        self, test_stream_input: EventstreamType, sequences_sample_size_two_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        plot_params = {"sample_size": 2}

        result = run_test_sample_size(
            test_stream_input, sequences_sample_size_two_corr, fit_params=fit_params, plot_params=plot_params
        )
        assert result[0], "Check all columns except 'sample_ids'"
        assert result[1], "Check sample_ids column"

    def test_sequences__precision(
        self, test_stream_input: EventstreamType, sequences_precision_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (1, 2)}
        plot_params = {"sample_size": None, "precision": 1}
        assert run_test(test_stream_input, sequences_precision_corr, fit_params=fit_params, plot_params=plot_params)

    def test_sequences__heatmap(
        self, test_stream_input: EventstreamType, sequences_sample_heatmap_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (2, 2)}
        plot_params = {"heatmap_cols": ["count_share"], "sample_size": None}
        assert run_test(
            test_stream_input, sequences_sample_heatmap_corr, fit_params=fit_params, plot_params=plot_params
        )

    def test_sequences__heatmap_groups(
        self, test_stream_input: EventstreamType, sequences_heatmap_groups_corr: pd.DataFrame
    ) -> None:
        fit_params = {"ngram_range": (2, 2), "groups": (["user1", "user2"], ["user3"])}

        plot_params = {"heatmap_cols": ("paths", "group_1"), "sample_size": None}
        assert run_test(
            test_stream_input, sequences_heatmap_groups_corr, fit_params=fit_params, plot_params=plot_params
        )
