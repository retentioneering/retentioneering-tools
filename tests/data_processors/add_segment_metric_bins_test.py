import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import PreprocessingConfigError


def _stream(paths, step="1h"):
    """Build an eventstream from {path_id: [events]}, one event per `step`."""
    rows = []
    for pid, events in paths.items():
        base = pd.Timestamp("2023-01-01 10:00")
        for i, event in enumerate(events):
            rows.append(
                {
                    "user_id": pid,
                    "event": event,
                    "timestamp": base + i * pd.Timedelta(step),
                }
            )
    return Eventstream(pd.DataFrame(rows))


@pytest.fixture()
def lengths():
    """Path lengths 1, 3, 6 and 12 events."""
    return _stream(
        {
            1: ["a"],
            2: ["a", "b", "c"],
            3: ["a", "b", "c", "d", "e", "f"],
            4: ["a"] * 12,
        }
    )


def _levels_by_path(stream, name):
    return (
        stream.df.groupby("user_id", observed=True)[name].first().astype(str).to_dict()
    )


class TestEdges:
    def test__n_cut_points_give_n_plus_one_bins(self, lengths):
        """Interior cut points, unlike pd.cut's outer edges: nothing falls out."""
        result = lengths.add_segment(
            "size",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [3, 10],
                "segment_levels": ["small", "medium", "large"],
            },
        )
        assert _levels_by_path(result, "size") == {
            1: "small",  # length 1  -> (-inf, 3)
            2: "medium",  # length 3  -> [3, 10)
            3: "medium",  # length 6  -> [3, 10)
            4: "large",  # length 12 -> [10, inf)
        }

    def test__bins_are_left_closed(self, lengths):
        """A value exactly on a cut point belongs to the bin above it."""
        result = lengths.add_segment(
            "size",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [3],
                "segment_levels": ["below", "from_three"],
            },
        )
        assert _levels_by_path(result, "size")[2] == "from_three"

    def test__auto_levels_are_interval_notation(self, lengths):
        result = lengths.add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "edges": [3, 10]}
        )
        assert result.get_segment_levels()["size"] == [
            "(-∞, 3)",
            "[10, ∞)",
            "[3, 10)",
        ]

    def test__every_path_gets_a_level(self, lengths):
        result = lengths.add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "edges": [3]}
        )
        assert result.df["size"].notna().all()


class TestQuantiles:
    def test__int_asks_for_that_many_bins(self, lengths):
        result = lengths.add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "quantiles": 4}
        )
        assert result.get_segment_levels()["size"] == ["q1", "q2", "q3", "q4"]
        assert len(set(_levels_by_path(result, "size").values())) == 4

    def test__list_gives_cut_points_so_n_points_are_n_plus_one_bins(self, lengths):
        result = lengths.add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "quantiles": [0.5]}
        )
        assert result.get_segment_levels()["size"] == ["q1", "q2"]

    def test__custom_quantiles_accept_segment_levels(self, lengths):
        result = lengths.add_segment(
            "size",
            metric_bins={
                "metric": {"metric": "length"},
                "quantiles": [0.25, 0.75],
                "segment_levels": ["fast", "typical", "slow"],
            },
        )
        assert set(result.get_segment_levels()["size"]) == {"fast", "typical", "slow"}

    def test__computed_over_the_stream_as_it_is_at_this_call(self, lengths):
        """Quantile boundaries depend on the population, so the order of
        add_segment and filter_paths in a chain matters. Documented, not a bug."""
        lengths = _stream({1: ["a"], 2: ["a"] * 2, 3: ["a"] * 3, 4: ["a"] * 100})
        condition = {
            "op": ">",
            "value": 2,
            "metric": "length",
        }
        before = lengths.add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "quantiles": [0.5]}
        ).filter_paths(condition)
        after = lengths.filter_paths(condition).add_segment(
            "size", metric_bins={"metric": {"metric": "length"}, "quantiles": [0.5]}
        )
        assert _levels_by_path(before, "size") != _levels_by_path(after, "size")


#: time_between is the one metric build_metrics leaves as NaN rather than
#: filling with 0 (metric_builder.build_metrics), so it is the only way a path
#: can genuinely have no value to bin.
_TIME_BETWEEN = {
    "metric": "time_between",
    "metric_args": {"start_event": "a", "end_event": "c"},
}


class TestUndefinedLevel:
    def test__paths_without_a_metric_value_get_undefined(self):
        """A path missing one of time_between's two events has no value — it
        still needs a level, since a segment column cannot hold NaN."""
        stream = _stream({1: ["a", "b"], 2: ["a", "b", "c"]})
        result = stream.add_segment(
            "gap", metric_bins={"metric": _TIME_BETWEEN, "edges": [1]}
        )
        assert _levels_by_path(result, "gap")[1] == "undefined"

    def test__undefined_is_not_counted_against_segment_levels(self):
        stream = _stream({1: ["a", "b"], 2: ["a", "b", "c"]})
        result = stream.add_segment(
            "gap",
            metric_bins={
                "metric": _TIME_BETWEEN,
                "edges": [1],
                "segment_levels": ["quick", "slow"],
            },
        )
        assert set(result.get_segment_levels()["gap"]) == {"slow", "undefined"}

    def test__undefined_is_rejected_as_an_explicit_level(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="undefined"):
            lengths.add_segment(
                "size",
                metric_bins={
                    "metric": {"metric": "length"},
                    "edges": [3],
                    "segment_levels": ["small", "undefined"],
                },
            )

    def test__undefined_can_be_renamed(self):
        stream = _stream({1: ["a", "b"], 2: ["a", "b", "c"]})
        result = stream.add_segment(
            "gap", metric_bins={"metric": _TIME_BETWEEN, "edges": [1]}
        ).rename_segment_levels("gap", {"undefined": "never_reached_c"})
        assert "never_reached_c" in result.get_segment_levels()["gap"]


class TestValidation:
    def test__level_count_mismatch_counts_bins_for_the_user(self, lengths):
        with pytest.raises(PreprocessingConfigError) as exc:
            lengths.add_segment(
                "size",
                metric_bins={
                    "metric": {"metric": "length"},
                    "edges": [3, 10],
                    "segment_levels": ["small", "large"],
                },
            )
        message = exc.value.message
        assert "2 cut point(s)" in message and "3 bins" in message

    def test__edges_and_quantiles_are_mutually_exclusive(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="exactly one"):
            lengths.add_segment(
                "size",
                metric_bins={
                    "metric": {"metric": "length"},
                    "edges": [3],
                    "quantiles": 2,
                },
            )

    def test__one_of_edges_or_quantiles_is_required(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="exactly one"):
            lengths.add_segment("size", metric_bins={"metric": {"metric": "length"}})

    def test__metric_is_required(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="metric"):
            lengths.add_segment("size", metric_bins={"edges": [3]})

    def test__unknown_key_is_rejected(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="unknown key"):
            lengths.add_segment(
                "size",
                metric_bins={"metric": {"metric": "length"}, "bins": [3], "labels": []},
            )

    def test__cut_points_must_increase(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="increasing"):
            lengths.add_segment(
                "size", metric_bins={"metric": {"metric": "length"}, "edges": [10, 3]}
            )

    def test__quantiles_must_be_between_zero_and_one(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="between 0 and 1"):
            lengths.add_segment(
                "size",
                metric_bins={"metric": {"metric": "length"}, "quantiles": [0.5, 1.5]},
            )

    def test__levels_must_be_unique(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="unique"):
            lengths.add_segment(
                "size",
                metric_bins={
                    "metric": {"metric": "length"},
                    "edges": [3],
                    "segment_levels": ["same", "same"],
                },
            )

    def test__multi_column_metric_is_rejected(self, lengths):
        """has_event_bulk expands into one column per event, so there is no
        single value to bin."""
        with pytest.raises(
            PreprocessingConfigError, match="exactly one value per path"
        ):
            lengths.add_segment(
                "size",
                metric_bins={
                    "metric": {
                        "metric": "has_event_bulk",
                        "metric_args": {"events": ["a", "b"]},
                    },
                    "edges": [0.5],
                },
            )

    def test__conflicts_with_another_mode(self, lengths):
        with pytest.raises(PreprocessingConfigError, match="At most one"):
            lengths.add_segment(
                "size",
                metric_bins={"metric": {"metric": "length"}, "edges": [3]},
                time_range=("2023-01-01", "2023-01-02"),
            )


class TestBinnedSegmentIsUsableDownstream:
    def test__can_be_used_as_a_diff_group(self, lengths):
        stream = lengths.add_segment(
            "size",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [5],
                "segment_levels": ["short", "long"],
            },
        )
        diff, g1, g2 = stream.transition_graph_data(
            edge_weight="count", diff=("size", "short", "long")
        )
        assert diff.shape == g1.shape == g2.shape

    def test__can_be_used_by_in_segment(self, lengths):
        stream = lengths.add_segment(
            "size",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [5],
                "segment_levels": ["short", "long"],
            },
        )
        filtered = stream.filter_paths(
            {
                "op": "=",
                "value": True,
                "metric": "in_segment",
                "metric_args": {"segment_name": "size", "segment_level": "long"},
            }
        )
        assert set(filtered.df["user_id"].unique()) == {3, 4}
