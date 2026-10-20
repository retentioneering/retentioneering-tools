import pandas as pd

from retentioneering.datasets.ecom import _generate_ecom, load_ecom
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


def _sessions_with_purchase_before_payment(df: pd.DataFrame) -> list[str]:
    # The state machine only reaches `purchase` from `payment_details`, so in
    # timestamp order every purchase session must show payment_details first.
    df = df.sort_values(["session_id", "timestamp"], kind="stable")
    bad = []
    for session_id, session in df.groupby("session_id", sort=False):
        events = session["event"].astype(str).tolist()
        if "purchase" not in events:
            continue
        if "payment_details" not in events[: events.index("purchase")]:
            bad.append(session_id)
    return bad


class TestEcomTimestampOrder:
    def test__generated_timestamps_follow_event_order(self) -> None:
        df = _generate_ecom(n_users=150, seed=7)

        assert (
            df.groupby("session_id")["timestamp"].nunique()
            == df.groupby("session_id").size()
        ).all()
        assert _sessions_with_purchase_before_payment(df) == []

    def test__bundled_dataset_follows_event_order(self) -> None:
        df = load_ecom(as_dataframe=True)

        assert _sessions_with_purchase_before_payment(df) == []
