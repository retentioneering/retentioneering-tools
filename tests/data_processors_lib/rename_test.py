from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import RenameParams, RenameProcessor
from retentioneering.eventstream import Eventstream
from retentioneering.eventstream.schema import RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase
from tests.data_processors_lib.fixtures.rename import (
    complex_dataset_for_rename,
    complex_expected_results,
    complex_rules,
    simple_dataset_for_rename,
    simple_expected_results,
    simple_rules,
)


class TestRename(ApplyTestBase):
    _Processor = RenameProcessor

    def test_rename_dataprocessor__simple(
        self,
        simple_dataset_for_rename: pd.DataFrame,
        simple_rules: list[dict[str, list[str]]],
        simple_expected_results: pd.DataFrame,
    ) -> None:
        actual = self._apply_dataprocessor(params=RenameParams(rules=simple_rules), source_df=simple_dataset_for_rename)
        assert pd.testing.assert_frame_equal(actual[simple_expected_results.columns], simple_expected_results) is None

    def test_rename_dataprocessor__complex(
        self,
        complex_dataset_for_rename: pd.DataFrame,
        complex_rules: list[dict[str, list[str]]],
        complex_expected_results: pd.DataFrame,
    ) -> None:
        actual = self._apply_dataprocessor(
            params=RenameParams(rules=complex_rules), source_df=complex_dataset_for_rename
        )
        assert pd.testing.assert_frame_equal(actual[complex_expected_results.columns], complex_expected_results) is None

    def test_rename__helper(
        self,
        simple_dataset_for_rename: pd.DataFrame,
        simple_rules: list[dict[str, str]],
        simple_expected_results: pd.DataFrame,
    ):
        source = Eventstream(simple_dataset_for_rename)

        actual = source.rename(rules=simple_rules)
        result_df = actual.to_dataframe()[simple_expected_results.columns].reset_index(drop=True)
        assert (
            pd.testing.assert_frame_equal(result_df[simple_expected_results.columns], simple_expected_results) is None
        )
