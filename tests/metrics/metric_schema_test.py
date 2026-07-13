"""Unit tests for the metric schema registry (metric_schema.py).

Note: `metric_builder.py`'s dispatch no longer routes through this registry
(see `metrics/condition_ast.py` / `MetricBuilder._parse_dict_config`'s explicit
per-metric dispatch) — this module is exercised standalone here, not as the
source of truth for `VALID_METRICS`.
"""

import pytest

from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.metrics.metric_schema import (
    METRIC_SCHEMAS,
    ValidationContext,
    parse_metric_args,
    validate_metric_args,
)


def make_ctx(available_events=(), segment_cols=(), segment_values=None):
    return ValidationContext(
        available_events=set(available_events),
        segment_cols=list(segment_cols),
        segment_values=(segment_values or (lambda col: set())),
    )


class TestZeroArgMetrics:
    @pytest.mark.parametrize("metric", ["length", "duration", "first_event_time"])
    def test_parse(self, metric):
        fields = parse_metric_args(metric, {})
        assert fields == {}
        assert METRIC_SCHEMAS[metric].metric_names(fields) == [metric]
        assert METRIC_SCHEMAS[metric].build_type == metric

    @pytest.mark.parametrize("metric", ["length", "duration", "first_event_time"])
    def test_validate_never_raises(self, metric):
        validate_metric_args(metric, {}, make_ctx())


class TestActiveDays:
    def test_parse_with_and_without_active_events(self):
        assert parse_metric_args("active_days", {}) == {"active_events": None}
        assert parse_metric_args("active_days", {"active_events": ["A"]}) == {
            "active_events": ["A"]
        }

    def test_metric_names_always_active_days(self):
        fields = parse_metric_args("active_days", {"active_events": "A"})
        assert METRIC_SCHEMAS["active_days"].metric_names(fields) == ["active_days"]

    def test_validate_never_raises(self):
        validate_metric_args(
            "active_days", {"active_events": "nonexistent"}, make_ctx()
        )


class TestEventListMetrics:
    @pytest.mark.parametrize(
        "metric,prefix", [("has_event", "has_event"), ("event_count", "event_count")]
    )
    def test_parse_wildcard_when_omitted(self, metric, prefix):
        fields = parse_metric_args(metric, {})
        assert fields["event_names"] is None
        assert METRIC_SCHEMAS[metric].metric_names(fields) is None

    @pytest.mark.parametrize(
        "metric,prefix", [("has_event", "has_event"), ("event_count", "event_count")]
    )
    def test_parse_single_and_list(self, metric, prefix):
        single = parse_metric_args(metric, {"events": "purchase"})
        assert single["event_names"] == ["purchase"]
        assert METRIC_SCHEMAS[metric].metric_names(single) == [f"{prefix}_purchase"]

        multi = parse_metric_args(metric, {"events": ["a", "b"]})
        assert multi["event_names"] == ["a", "b"]
        assert METRIC_SCHEMAS[metric].metric_names(multi) == [
            f"{prefix}_a",
            f"{prefix}_b",
        ]

    @pytest.mark.parametrize("metric", ["has_event", "event_count"])
    def test_parse_rejects_bad_type(self, metric):
        with pytest.raises(InvalidMetricConfigError, match="events"):
            parse_metric_args(metric, {"events": {"not": "valid"}})

    @pytest.mark.parametrize("metric", ["has_event", "event_count"])
    def test_validate_missing_event_raises(self, metric):
        with pytest.raises(InvalidMetricConfigError, match="typo_event"):
            validate_metric_args(
                metric, {"events": "typo_event"}, make_ctx(available_events=["a", "b"])
            )

    @pytest.mark.parametrize("metric", ["has_event", "event_count"])
    def test_validate_wildcard_never_raises(self, metric):
        validate_metric_args(metric, {}, make_ctx(available_events=["a"]))


class TestMatchesPattern:
    def test_parse_and_metric_names(self):
        fields = parse_metric_args("matches_pattern", {"pattern": "a->b"})
        assert fields == {"pattern": "a->b"}
        assert METRIC_SCHEMAS["matches_pattern"].metric_names(fields) == [
            "matches_pattern_a->b"
        ]

    def test_parse_requires_pattern(self):
        with pytest.raises(InvalidMetricConfigError, match="requires 'pattern'"):
            parse_metric_args("matches_pattern", {})

    def test_validate_requires_pattern(self):
        with pytest.raises(InvalidMetricConfigError, match="requires 'pattern'"):
            validate_metric_args("matches_pattern", {}, make_ctx())


class TestTimeBetween:
    def test_parse_and_build_type(self):
        fields = parse_metric_args(
            "time_between", {"start_event": "a", "end_event": "b"}
        )
        assert fields == {"start_event": "a", "end_event": "b"}
        assert METRIC_SCHEMAS["time_between"].build_type == "time_from_to"
        assert METRIC_SCHEMAS["time_between"].metric_names(fields) == [
            "time_from_a_to_b"
        ]

    @pytest.mark.parametrize(
        "metric_args", [{}, {"start_event": "a"}, {"end_event": "b"}]
    )
    def test_parse_requires_both_events(self, metric_args):
        with pytest.raises(InvalidMetricConfigError, match="start_event.*end_event"):
            parse_metric_args("time_between", metric_args)

    def test_validate_missing_event_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="typo_event"):
            validate_metric_args(
                "time_between",
                {"start_event": "typo_event", "end_event": "b"},
                make_ctx(available_events=["a", "b"]),
            )

    def test_validate_allows_synthetic_path_start_end(self):
        validate_metric_args(
            "time_between",
            {"start_event": "path_start", "end_event": "path_end"},
            make_ctx(available_events=["a"]),
        )


class TestInSegment:
    def test_parse_scalar_and_list_segment_value(self):
        scalar = parse_metric_args(
            "in_segment", {"segment_name": "platform", "segment_value": "ios"}
        )
        assert scalar["segment_values"] == ["ios"]
        assert METRIC_SCHEMAS["in_segment"].metric_names(scalar) == [
            "in_segment_platform_ios_any"
        ]

        multi = parse_metric_args(
            "in_segment",
            {"segment_name": "platform", "segment_value": ["ios", "android"]},
        )
        assert multi["segment_values"] == ["ios", "android"]

    def test_parse_none_segment_value_defers_metric_names(self):
        fields = parse_metric_args("in_segment", {"segment_name": "platform"})
        assert fields["segment_values"] is None
        assert METRIC_SCHEMAS["in_segment"].metric_names(fields) is None

    def test_parse_requires_segment_name(self):
        with pytest.raises(InvalidMetricConfigError, match="segment_name"):
            parse_metric_args("in_segment", {})

    def test_parse_rejects_invalid_mode(self):
        with pytest.raises(InvalidMetricConfigError, match="invalid mode"):
            parse_metric_args(
                "in_segment", {"segment_name": "platform", "mode": "bogus"}
            )

    def test_parse_rejects_empty_segment_value_list(self):
        with pytest.raises(InvalidMetricConfigError, match="non-empty"):
            parse_metric_args(
                "in_segment", {"segment_name": "platform", "segment_value": []}
            )

    def test_parse_event_share_requires_threshold(self):
        with pytest.raises(InvalidMetricConfigError, match="threshold"):
            parse_metric_args(
                "in_segment",
                {
                    "segment_name": "platform",
                    "segment_value": "ios",
                    "mode": "event_share",
                },
            )

    def test_validate_unknown_segment_column_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="platform"):
            validate_metric_args(
                "in_segment",
                {"segment_name": "platform"},
                make_ctx(segment_cols=["country"]),
            )

    def test_validate_unknown_segment_value_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="ios"):
            validate_metric_args(
                "in_segment",
                {"segment_name": "platform", "segment_value": "ios"},
                make_ctx(
                    segment_cols=["platform"], segment_values=lambda col: {"android"}
                ),
            )

    def test_validate_event_share_threshold_out_of_range_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="between 0 and 1"):
            validate_metric_args(
                "in_segment",
                {
                    "segment_name": "platform",
                    "segment_value": "ios",
                    "mode": "event_share",
                    "threshold": 1.5,
                },
                make_ctx(segment_cols=["platform"], segment_values=lambda col: {"ios"}),
            )


class TestUnknownMetric:
    def test_parse_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="Unknown metric"):
            parse_metric_args("not_a_real_metric", {})

    def test_validate_raises(self):
        with pytest.raises(InvalidMetricConfigError, match="Unknown metric"):
            validate_metric_args("not_a_real_metric", {}, make_ctx())
