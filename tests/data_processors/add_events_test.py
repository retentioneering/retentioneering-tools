import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    return pd.DataFrame([
        ["user_1", "A", "2020-01-01 00:00:00"],
        ["user_1", "B", "2020-01-02 00:00:00"],
        ["user_1", "C", "2020-01-03 00:00:00"],
        ["user_2", "A", "2020-01-01 00:00:00"],
        ["user_2", "B", "2020-01-02 00:00:00"],
    ], columns=["user_id", "event", "timestamp"])


SCHEMA_WITH_EVENT_TYPE = {"event_type": "event_type"}


# ---------------------------------------------------------------------------
# Mode 1: source_events
# ---------------------------------------------------------------------------

class TestAddEventsBySourceEvents:

    def test__single_source_event(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(new_event_name="S", source_events=["A"])

        expected = Eventstream(pd.DataFrame([
            ["user_1", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_1", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_1", "B",    "raw",       "2020-01-02 00:00:00"],
            ["user_1", "C",    "raw",       "2020-01-03 00:00:00"],
            ["user_2", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_2", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_2", "B",    "raw",       "2020-01-02 00:00:00"],
        ], columns=["user_id", "event", "event_type", "timestamp"]), SCHEMA_WITH_EVENT_TYPE)

        assert res.equals(expected)

    def test__multiple_source_events(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(new_event_name="S", source_events=["A", "B"])

        expected = Eventstream(pd.DataFrame([
            ["user_1", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_1", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_1", "B",    "raw",       "2020-01-02 00:00:00"],
            ["user_1", "S",    "synthetic", "2020-01-02 00:00:00"],
            ["user_1", "C",    "raw",       "2020-01-03 00:00:00"],
            ["user_2", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_2", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_2", "B",    "raw",       "2020-01-02 00:00:00"],
            ["user_2", "S",    "synthetic", "2020-01-02 00:00:00"],
        ], columns=["user_id", "event", "event_type", "timestamp"]), SCHEMA_WITH_EVENT_TYPE)

        assert res.equals(expected)

    def test__empty_source_events_is_noop(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(new_event_name="S", source_events=[])

        assert res.equals(stream)

    def test__all_columns_copied_from_source(self) -> None:
        df = pd.DataFrame([
            ["user_1", "A", "2020-01-01 00:00:00", "US"],
            ["user_1", "B", "2020-01-02 00:00:00", "US"],
        ], columns=["user_id", "event", "timestamp", "country"])
        stream = Eventstream(df, {"custom_cols": ["country"]})

        res = stream.add_events(new_event_name="S", source_events=["A"])

        synthetic_rows = res.df[res.df["event"] == "S"]
        assert len(synthetic_rows) == 1
        assert synthetic_rows.iloc[0]["country"] == "US"
        assert str(synthetic_rows.iloc[0]["timestamp"]) == "2020-01-01 00:00:00"

    def test__synthetic_event_comes_after_source(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(new_event_name="S", source_events=["A"])

        events_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()
        assert events_user1.index("A") < events_user1.index("S")

    def test__synthetic_event_comes_before_next_raw_event(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(new_event_name="S", source_events=["A"])

        events_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()
        assert events_user1.index("S") < events_user1.index("B")

    def test__unknown_source_event_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="S", source_events=["UNKNOWN"])

    def test__source_events_not_list_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="S", source_events="A")

    def test__source_events_non_string_elements_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="S", source_events=[1, 2])


# ---------------------------------------------------------------------------
# Mode 2: sql
# ---------------------------------------------------------------------------

class TestAddEventsBySql:

    def test__sql_basic(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(
            new_event_name="S",
            sql="SELECT * FROM eventstream WHERE event = 'A'",
        )

        expected = Eventstream(pd.DataFrame([
            ["user_1", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_1", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_1", "B",    "raw",       "2020-01-02 00:00:00"],
            ["user_1", "C",    "raw",       "2020-01-03 00:00:00"],
            ["user_2", "A",    "raw",       "2020-01-01 00:00:00"],
            ["user_2", "S",    "synthetic", "2020-01-01 00:00:00"],
            ["user_2", "B",    "raw",       "2020-01-02 00:00:00"],
        ], columns=["user_id", "event", "event_type", "timestamp"]), SCHEMA_WITH_EVENT_TYPE)

        assert res.equals(expected)

    def test__sql_wrong_columns_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(
                new_event_name="S",
                sql="SELECT user_id, event FROM eventstream",
            )

    def test__sql_not_string_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="S", sql=123)


# ---------------------------------------------------------------------------
# Mode 3: churn
# ---------------------------------------------------------------------------

class TestAddEventsByChurn:

    def get_churn_df(self):
        """
        user_1: A(Jan1), B(Jan10), C(Mar1)  — B→C gap 50d, C is dataset max
        user_2: A(Jan1), B(Jan10)           — B is last, dataset_end(Mar1) - Jan10 = 50d
        user_3: A(Jan1), B(Feb20)           — A→B gap 50d; B is last, dataset_end - Feb20 = 9d
        """
        return pd.DataFrame([
            ["user_1", "A", "2020-01-01"],
            ["user_1", "B", "2020-01-10"],
            ["user_1", "C", "2020-03-01"],
            ["user_2", "A", "2020-01-01"],
            ["user_2", "B", "2020-01-10"],
            ["user_3", "A", "2020-01-01"],
            ["user_3", "B", "2020-02-20"],
        ], columns=["user_id", "event", "timestamp"])

    def test__churn_any_event(self) -> None:
        """Without active_events: any event resets the inactivity clock."""
        stream = Eventstream(self.get_churn_df())

        res = stream.add_events(new_event_name="churn", churn={"inactivity_days": 30})

        expected = Eventstream(pd.DataFrame([
            ["user_1", "A",     "raw",       "2020-01-01"],
            ["user_1", "B",     "raw",       "2020-01-10"],
            ["user_1", "churn", "synthetic", "2020-01-10"],
            ["user_1", "C",     "raw",       "2020-03-01"],
            ["user_2", "A",     "raw",       "2020-01-01"],
            ["user_2", "B",     "raw",       "2020-01-10"],
            ["user_2", "churn", "synthetic", "2020-01-10"],
            ["user_3", "A",     "raw",       "2020-01-01"],
            ["user_3", "churn", "synthetic", "2020-01-01"],
            ["user_3", "B",     "raw",       "2020-02-20"],
        ], columns=["user_id", "event", "event_type", "timestamp"]), SCHEMA_WITH_EVENT_TYPE)

        assert res.equals(expected)

    def test__churn_dataset_edge_not_marked(self) -> None:
        """The last event in the dataset (C for user_1) is never marked as churn
        because its gap to dataset_end is zero."""
        stream = Eventstream(self.get_churn_df())

        res = stream.add_events(new_event_name="churn", churn={"inactivity_days": 30})

        last_event_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()[-1]
        assert last_event_user1 == "C"

    def test__churn_with_active_events(self) -> None:
        """Only active_events reset the clock; non-active events are ignored."""
        df = pd.DataFrame([
            ["user_1", "login",    "2020-01-01"],
            ["user_1", "purchase", "2020-01-10"],
            ["user_1", "login",    "2020-01-15"],
            ["user_1", "login",    "2020-02-01"],
            ["user_1", "purchase", "2020-03-20"],  # dataset max
        ], columns=["user_id", "event", "timestamp"])
        stream = Eventstream(df)

        res = stream.add_events(
            new_event_name="churn",
            churn={"inactivity_days": 30, "active_events": ["purchase"]},
        )

        # purchase(Jan10) → next purchase(Mar20): gap 70d > 30 → churn after purchase(Jan10)
        # purchase(Mar20) is dataset max → gap 0 → no churn
        events_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()
        churn_idx = events_user1.index("churn")
        purchase_idx = events_user1.index("purchase")
        assert purchase_idx < churn_idx

    def test__churn_empty_active_events_is_noop(self) -> None:
        stream = Eventstream(self.get_churn_df())

        res = stream.add_events(new_event_name="churn", churn={"inactivity_days": 30, "active_events": []})

        assert res.equals(stream)

    def test__churn_missing_inactivity_days_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(new_event_name="churn", churn={})

    def test__churn_negative_inactivity_days_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(
                new_event_name="churn", churn={"inactivity_days": -1}
            )

    def test__churn_not_dict_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(new_event_name="churn", churn=30)

    def test__churn_active_events_not_list_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(
                new_event_name="churn", churn={"inactivity_days": 30, "active_events": "purchase"}
            )


# ---------------------------------------------------------------------------
# Common validation
# ---------------------------------------------------------------------------

class TestAddEventsValidation:

    def test__no_mode_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="S")

    def test__multiple_modes_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(
                new_event_name="S",
                source_events=["A"],
                churn={"inactivity_days": 30},
            )

    def test__new_event_name_not_string_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name=123, source_events=["A"])

    def test__new_event_name_empty_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(new_event_name="", source_events=["A"])
