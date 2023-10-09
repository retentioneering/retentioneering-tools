from __future__ import annotations

import pandas as pd
import pytest
from pandas.core.common import flatten
from pydantic import ValidationError

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.funnel import Funnel
from tests.tooling.fixtures.funnel_corr import (
    funnel_closed_corr,
    funnel_closed_segment_names_corr,
    funnel_hybrid_corr,
    funnel_hybrid_segments_corr,
    funnel_open_corr,
    funnel_open_stage_names_corr,
)
from tests.tooling.fixtures.funnel_input import test_stream

CONV_USERS = [1, 2, 3, 7]
NON_CONV_USERS = [4, 5, 6, 8]


def run_test(stream: EventstreamType, correct_res: pd.DataFrame, params: dict) -> bool:
    funnel = Funnel(eventstream=stream)
    funnel.fit(**params)
    res = funnel.values
    correct_res = correct_res.round(2)
    return correct_res.compare(res).shape == (0, 0)


class TestFunnel:
    def test_funnel__open(self, test_stream: EventstreamType, funnel_open_corr: pd.DataFrame) -> None:
        params = {"funnel_type": "open", "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        assert run_test(test_stream, funnel_open_corr, params)

    def test_funnel__open_stage_names(
        self, test_stream: EventstreamType, funnel_open_stage_names_corr: pd.DataFrame
    ) -> None:
        params = {
            "funnel_type": "open",
            "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"],
            "stage_names": ["catalog", "product", "cart", "payment_done"],
        }

        assert run_test(test_stream, funnel_open_stage_names_corr, params)

    def test_funnel__closed(self, test_stream: EventstreamType, funnel_closed_corr: pd.DataFrame) -> None:
        params = {
            "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"],
            "funnel_type": "closed",
        }

        assert run_test(test_stream, funnel_closed_corr, params)

    def test_funnel__hybrid(self, test_stream: EventstreamType, funnel_hybrid_corr: pd.DataFrame) -> None:
        params = {
            "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"],
            "stage_names": None,
            "funnel_type": "hybrid",
        }

        assert run_test(test_stream, funnel_hybrid_corr, params)

    def test_funnel__hybrid_segments(
        self, test_stream: EventstreamType, funnel_hybrid_segments_corr: pd.DataFrame
    ) -> None:
        params = {
            "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"],
            "funnel_type": "hybrid",
            "segments": (CONV_USERS, NON_CONV_USERS),
        }

        correct_result = {
            "group 0": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [3, 2, 2, 2]},
            "group 1": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [3, 2, 2, 2]},
        }
        assert run_test(test_stream, funnel_hybrid_segments_corr, params)

    def test_funnel__closed_segment_names(
        self, test_stream: EventstreamType, funnel_closed_segment_names_corr: pd.DataFrame
    ) -> None:
        params = {
            "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"],
            "funnel_type": "closed",
            "segments": (CONV_USERS, NON_CONV_USERS),
            "segment_names": ["conv_users", "non_conv_users"],
        }

        assert run_test(test_stream, funnel_closed_segment_names_corr, params)

    def test_params_model__incorrect_funnel_type(self, test_stream):
        with pytest.raises(ValueError):
            p = Funnel(eventstream=test_stream)
            p.fit(stages=["catalog", "cart", "payment_done"], funnel_type="check_me")
