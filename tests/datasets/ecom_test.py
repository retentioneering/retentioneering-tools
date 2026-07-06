import pandas as pd

from retentioneering.datasets.ecom import load_ecom
from retentioneering.eventstream.eventstream import Eventstream


class TestLoadEcom:
    def test__default_returns_configured_eventstream(self) -> None:
        stream = load_ecom()

        assert isinstance(stream, Eventstream)
        assert stream.schema.path_cols == ["user_id", "session_id"]
        assert stream.schema.segment_cols == [
            "platform",
            "acquisition_channel",
            "user_cohort",
            "user_lifecycle",
        ]

    def test__as_dataframe_false_returns_eventstream(self) -> None:
        stream = load_ecom(as_dataframe=False)
        assert isinstance(stream, Eventstream)

    def test__as_dataframe_true_returns_dataframe(self) -> None:
        df = load_ecom(as_dataframe=True)
        assert isinstance(df, pd.DataFrame)

    def test__eventstream_wraps_same_data_as_dataframe(self) -> None:
        df = load_ecom(as_dataframe=True)
        stream = load_ecom()

        assert len(stream.df) == len(df)
