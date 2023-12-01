from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.sequences_corr import sequences_eventstream_corr
from tests.eventstream.tooling.fixtures.sequences_input import test_stream


class TestEventstreamSequences:
    def test_sequences_eventstream__basic(self, test_stream: EventstreamType, sequences_eventstream_corr: pd.DataFrame):
        result = test_stream.sequences(
            ngram_range=(2, 2),
            groups=(["user1", "user2"], ["user3"]),
            group_names=("pay", "no_pay"),
            weight_col="user_id",
            metrics=["paths", "count"],
            threshold=[("paths", "pay"), 1],
            sorting=(("count", "delta_abs"), True),
            heatmap_cols=(["count", "pay"]),
            sample_size=None,
            precision=1,
            show_plot=True,
        ).values
        corr = sequences_eventstream_corr

        assert result.compare(corr).shape == (0, 0)
