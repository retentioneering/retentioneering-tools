import numpy as np
import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError


def sample():
    return Eventstream(
        pd.DataFrame(
            {
                "user_id": ["a", "a", "b", "b", "c"],
                "event": ["open", "buy", "buy", "buy", "open"],
                "timestamp": pd.date_range("2026-01-01", periods=5, freq="s"),
                "amount": [np.nan, 12.0, 3.0, -1.0, np.nan],
            }
        ),
        schema={"custom_cols": ["amount"]},
    )


SPEC = {
    "metric": "event_value_sum",
    "metric_args": {"event": "buy", "value_col": "amount"},
}


def test_numeric_sum_and_structural_zero():
    result = sample().get_metrics([SPEC]).iloc[:, 0]
    assert result.to_dict() == {"a": 12.0, "b": 2.0, "c": 0.0}


@pytest.mark.parametrize("bad", [np.nan, np.inf, "missing"])
def test_matching_invalid_amount_rejected(bad):
    frame = sample().to_dataframe()
    frame["amount"] = frame.amount.astype(object)
    frame.loc[frame.event.eq("buy"), "amount"] = bad
    stream = Eventstream(frame, schema={"custom_cols": ["amount"]}, preprocess=False)
    with pytest.raises(InvalidMetricConfigError):
        stream.get_metrics([SPEC])


def test_shared_parser_rejects_unknown_argument():
    with pytest.raises(InvalidMetricConfigError, match="unknown"):
        sample().get_metrics(
            [{**SPEC, "metric_args": {**SPEC["metric_args"], "typo": True}}]
        )
