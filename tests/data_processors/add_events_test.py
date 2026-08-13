import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def get_df():
    return pd.DataFrame(
        [
            ["user_1", "A", "2020-01-01 00:00:00"],
            ["user_1", "B", "2020-01-02 00:00:00"],
            ["user_1", "C", "2020-01-03 00:00:00"],
            ["user_2", "A", "2020-01-01 00:00:00"],
            ["user_2", "B", "2020-01-02 00:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )


SCHEMA_WITH_EVENT_TYPE = {"event_type": "event_type"}


# ---------------------------------------------------------------------------
# Mode 1: source_events
# ---------------------------------------------------------------------------


class TestAddEventsBySourceEvents:
    def test__single_source_event(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(name="S", source_events=["A"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_1", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_1", "B", "raw", "2020-01-02 00:00:00"],
                    ["user_1", "C", "raw", "2020-01-03 00:00:00"],
                    ["user_2", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_2", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_2", "B", "raw", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "event_type", "timestamp"],
            ),
            SCHEMA_WITH_EVENT_TYPE,
        )

        assert res.equals(expected)

    def test__multiple_source_events(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(name="S", source_events=["A", "B"])

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_1", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_1", "B", "raw", "2020-01-02 00:00:00"],
                    ["user_1", "S", "synthetic", "2020-01-02 00:00:00"],
                    ["user_1", "C", "raw", "2020-01-03 00:00:00"],
                    ["user_2", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_2", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_2", "B", "raw", "2020-01-02 00:00:00"],
                    ["user_2", "S", "synthetic", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "event_type", "timestamp"],
            ),
            SCHEMA_WITH_EVENT_TYPE,
        )

        assert res.equals(expected)

    def test__empty_source_events_is_noop(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(name="S", source_events=[])

        assert res.equals(stream)

    def test__all_columns_copied_from_source(self) -> None:
        df = pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01 00:00:00", "US"],
                ["user_1", "B", "2020-01-02 00:00:00", "US"],
            ],
            columns=["user_id", "event", "timestamp", "country"],
        )
        stream = Eventstream(df, {"custom_cols": ["country"]})

        res = stream.add_events(name="S", source_events=["A"])

        synthetic_rows = res.df[res.df["event"] == "S"]
        assert len(synthetic_rows) == 1
        assert synthetic_rows.iloc[0]["country"] == "US"
        assert str(synthetic_rows.iloc[0]["timestamp"]) == "2020-01-01 00:00:00"

    def test__synthetic_event_comes_before_source(self) -> None:
        """A synthetic event marks the moment its source opens, so it precedes it."""
        stream = Eventstream(get_df())

        res = stream.add_events(name="S", source_events=["A"])

        events_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()
        assert events_user1.index("S") < events_user1.index("A")

    def test__synthetic_event_comes_before_next_raw_event(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(name="S", source_events=["A"])

        events_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()
        assert events_user1.index("S") < events_user1.index("B")

    def test__unknown_source_event_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="S", source_events=["UNKNOWN"])

    def test__source_events_not_list_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="S", source_events="A")

    def test__source_events_non_string_elements_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="S", source_events=[1, 2])


# ---------------------------------------------------------------------------
# Mode 2: sql
# ---------------------------------------------------------------------------


class TestAddEventsBySql:
    def test__sql_basic(self) -> None:
        stream = Eventstream(get_df())

        res = stream.add_events(
            name="S",
            sql="SELECT * FROM eventstream WHERE event = 'A'",
        )

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_1", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_1", "B", "raw", "2020-01-02 00:00:00"],
                    ["user_1", "C", "raw", "2020-01-03 00:00:00"],
                    ["user_2", "A", "raw", "2020-01-01 00:00:00"],
                    ["user_2", "S", "synthetic", "2020-01-01 00:00:00"],
                    ["user_2", "B", "raw", "2020-01-02 00:00:00"],
                ],
                columns=["user_id", "event", "event_type", "timestamp"],
            ),
            SCHEMA_WITH_EVENT_TYPE,
        )

        assert res.equals(expected)

    def test__sql_wrong_columns_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(
                name="S",
                sql="SELECT user_id, event FROM eventstream",
            )

    def test__sql_not_string_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="S", sql=123)


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
        return pd.DataFrame(
            [
                ["user_1", "A", "2020-01-01"],
                ["user_1", "B", "2020-01-10"],
                ["user_1", "C", "2020-03-01"],
                ["user_2", "A", "2020-01-01"],
                ["user_2", "B", "2020-01-10"],
                ["user_3", "A", "2020-01-01"],
                ["user_3", "B", "2020-02-20"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

    def test__churn_any_event(self) -> None:
        """Without active_events: any event resets the inactivity clock."""
        stream = Eventstream(self.get_churn_df())

        res = stream.add_events(name="churn", churn={"inactivity_days": 30})

        expected = Eventstream(
            pd.DataFrame(
                [
                    ["user_1", "A", "raw", "2020-01-01"],
                    ["user_1", "B", "raw", "2020-01-10"],
                    ["user_1", "churn", "churn", "2020-01-10"],
                    ["user_1", "C", "raw", "2020-03-01"],
                    ["user_2", "A", "raw", "2020-01-01"],
                    ["user_2", "B", "raw", "2020-01-10"],
                    ["user_2", "churn", "churn", "2020-01-10"],
                    ["user_3", "A", "raw", "2020-01-01"],
                    ["user_3", "churn", "churn", "2020-01-01"],
                    ["user_3", "B", "raw", "2020-02-20"],
                ],
                columns=["user_id", "event", "event_type", "timestamp"],
            ),
            SCHEMA_WITH_EVENT_TYPE,
        )

        assert res.equals(expected)

    def test__churn_dataset_edge_not_marked(self) -> None:
        """The last event in the dataset (C for user_1) is never marked as churn
        because its gap to dataset_end is zero."""
        stream = Eventstream(self.get_churn_df())

        res = stream.add_events(name="churn", churn={"inactivity_days": 30})

        last_event_user1 = res.df[res.df["user_id"] == "user_1"]["event"].tolist()[-1]
        assert last_event_user1 == "C"

    def test__churn_with_active_events(self) -> None:
        """Only active_events reset the clock; non-active events are ignored."""
        df = pd.DataFrame(
            [
                ["user_1", "login", "2020-01-01"],
                ["user_1", "purchase", "2020-01-10"],
                ["user_1", "login", "2020-01-15"],
                ["user_1", "login", "2020-02-01"],
                ["user_1", "purchase", "2020-03-20"],  # dataset max
            ],
            columns=["user_id", "event", "timestamp"],
        )
        stream = Eventstream(df)

        res = stream.add_events(
            name="churn",
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

        res = stream.add_events(
            name="churn", churn={"inactivity_days": 30, "active_events": []}
        )

        assert res.equals(stream)

    def test__churn_missing_inactivity_days_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(name="churn", churn={})

    def test__churn_negative_inactivity_days_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(
                name="churn", churn={"inactivity_days": -1}
            )

    def test__churn_not_dict_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(name="churn", churn=30)

    def test__churn_active_events_not_list_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_churn_df()).add_events(
                name="churn",
                churn={"inactivity_days": 30, "active_events": "purchase"},
            )

    def test__churn_event_carries_its_own_type(self) -> None:
        """Churn closes a stretch of inactivity: it follows the event that
        started it and still precedes `path_end`."""
        res = (
            Eventstream(self.get_churn_df())
            .add_events(name="churn", churn={"inactivity_days": 30})
            .add_start_end_events()
        )

        churn_rows = res.df[res.df["event"] == "churn"]
        assert set(churn_rows["event_type"]) == {"churn"}

        u1 = res.to_dataframe(exclude_start_end=False)
        u1 = u1[u1["user_id"] == "user_1"]["event"].tolist()
        assert u1.index("B") < u1.index("churn") < u1.index("path_end")


# ---------------------------------------------------------------------------
# Mode 4: anchor
# ---------------------------------------------------------------------------


class TestAddEventsByAnchor:
    def get_anchor_df(self):
        """
        user_1: the first cart is abandoned for more browsing, the second is the
                one checkout actually followed.
        user_2: two separate checkout attempts, and a cart between them that
                led nowhere.
        user_3: never reaches checkout.
        """
        rows = []
        paths = {
            "user_1": ["main", "cart", "catalog", "cart", "checkout", "pay"],
            "user_2": ["cart", "checkout", "cart", "catalog", "cart", "checkout"],
            "user_3": ["main", "cart", "catalog"],
        }
        for path, events in paths.items():
            for i, event in enumerate(events):
                rows.append(
                    [path, event, f"2020-01-01 00:{i:02d}:00"],
                )
        return pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])

    CHECKOUT = "cart->[^cart]*->checkout"

    def events_of(self, stream, path):
        df = stream.to_dataframe()
        return df[df["user_id"] == path]["event"].tolist()

    def test__anchor_marks_the_matched_position_not_the_event_name(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M", anchor={"pattern": self.CHECKOUT, "at": "start"}
        )

        # the *second* cart — the first is followed by another cart
        assert self.events_of(res, "user_1") == [
            "main",
            "cart",
            "catalog",
            "M",
            "cart",
            "checkout",
            "pay",
        ]
        # no checkout, no marker
        assert self.events_of(res, "user_3") == ["main", "cart", "catalog"]

    def test__anchor_marks_once_per_path_by_default(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M", anchor={"pattern": self.CHECKOUT, "at": "start"}
        )

        assert self.events_of(res, "user_2").count("M") == 1

    def test__occurrence_all_marks_every_valid_position(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M",
            anchor={"pattern": self.CHECKOUT, "at": "start", "occurrence": "all"},
        )

        # both carts that checkout followed, but not the middle one
        assert self.events_of(res, "user_2") == [
            "M",
            "cart",
            "checkout",
            "cart",
            "catalog",
            "M",
            "cart",
            "checkout",
        ]

    def test__marker_precedes_its_anchor_row(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(name="M", anchor="checkout")

        marker = res.df[res.df["event"] == "M"].iloc[0]
        anchor = res.df[
            (res.df["user_id"] == marker["user_id"]) & (res.df["event"] == "checkout")
        ].iloc[0]
        assert marker["timestamp"] == anchor["timestamp"]
        assert marker["subindex"] < anchor["subindex"]
        assert self.events_of(res, "user_1")[-3:] == ["M", "checkout", "pay"]

    def test__bare_string_anchor(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(name="M", anchor="checkout")

        assert self.events_of(res, "user_1").index("M") == 4

    def test__step_offset_moves_the_marker(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M", anchor={"pattern": "checkout", "offset": -2}
        )

        assert self.events_of(res, "user_1") == [
            "main",
            "cart",
            "M",
            "catalog",
            "cart",
            "checkout",
            "pay",
        ]

    def test__offset_past_the_path_boundary_clamps(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M", anchor={"pattern": "checkout", "offset": -99}
        )

        assert self.events_of(res, "user_1")[0] == "M"

    def test__time_offset_rounds_by_sign_without_offset_side(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M", anchor={"pattern": "cart", "offset": "90s"}
        )

        # cart at 00:01 + 90s = 00:02:30 → the first event at or after the mark
        assert self.events_of(res, "user_1") == [
            "main",
            "cart",
            "catalog",
            "M",
            "cart",
            "checkout",
            "pay",
        ]

    def test__offset_side_overrides_the_sign_rule(self) -> None:
        res = Eventstream(self.get_anchor_df()).add_events(
            name="M",
            anchor={"pattern": "cart", "offset": "90s", "offset_side": "end"},
        )

        # same mark, rounded back instead: the last event at or before 00:02:30
        assert self.events_of(res, "user_1") == [
            "main",
            "cart",
            "M",
            "catalog",
            "cart",
            "checkout",
            "pay",
        ]

    def test__path_col_override(self) -> None:
        df = self.get_anchor_df()
        df["session_id"] = df["user_id"] + "_s" + (df.index // 3).astype(str)
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})

        res = stream.add_events(name="M", anchor="cart", path_col="session_id")

        # one marker per session that has a cart, not one per user
        assert len(res.df[res.df["event"] == "M"]) == len(
            res.df[res.df["event"] == "cart"].drop_duplicates("session_id")
        )

    def test__marker_is_addressable_by_other_tools(self) -> None:
        """The point of the mode: a position becomes an event name."""
        res = Eventstream(self.get_anchor_df()).add_events(
            name="checkout_cart", anchor={"pattern": self.CHECKOUT, "at": "start"}
        )

        blocks = res.step_matrix_data(path_pattern="checkout_cart", max_steps=2)
        centred = blocks[0]
        assert centred.loc["checkout_cart", 0] == 1.0
        assert centred.loc["cart", 1] == 1.0

    def test__anchor_list_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError, match="not a list"):
            Eventstream(self.get_anchor_df()).add_events(
                name="M", anchor=["cart", "checkout"]
            )

    def test__unknown_anchor_event_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_anchor_df()).add_events(name="M", anchor="nope")

    def test__bad_occurrence_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_anchor_df()).add_events(
                name="M", anchor={"pattern": "cart", "occurrence": "second"}
            )

    def test__bad_offset_side_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_anchor_df()).add_events(
                name="M", anchor={"pattern": "cart", "offset_side": "middle"}
            )

    def test__unknown_path_col_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(self.get_anchor_df()).add_events(
                name="M", anchor="cart", path_col="nope"
            )


# ---------------------------------------------------------------------------
# Common validation
# ---------------------------------------------------------------------------


class TestAddEventsValidation:
    def test__no_mode_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="S")

    def test__multiple_modes_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(
                name="S",
                source_events=["A"],
                churn={"inactivity_days": 30},
            )

    def test__name_not_string_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name=123, source_events=["A"])

    def test__name_empty_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError):
            Eventstream(get_df()).add_events(name="", source_events=["A"])

    def test__name_with_path_delimiter_raises(self) -> None:
        with pytest.raises(PreprocessingConfigError, match="add->cart"):
            Eventstream(get_df()).add_events(name="add->cart", source_events=["A"])
