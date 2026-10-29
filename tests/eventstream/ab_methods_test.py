"""``Eventstream.get_ab_test`` / ``get_ab_bootstrap``: thin wrappers over ``tools/ab_test.py``."""

import inspect

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.ab_test import COLUMNS, ABTest

DIFF = ("arm", "treatment", "control")
METRICS = [
    {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order"}}
]


def _stream() -> Eventstream:
    rows = []
    for i in range(300):
        arm = "treatment" if i % 2 else "control"
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i % 10)
        rows.append([f"u{i}", "visit", day, arm])
        if i % 5 in (0, 1):
            rows.append([f"u{i}", "order", day + pd.Timedelta(hours=1), arm])
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
    return Eventstream(df, {"segment_cols": ["arm"]})


class TestGetAbTest:
    def test_matches_the_tool(self) -> None:
        stream = _stream()
        out = stream.get_ab_test(DIFF, METRICS, correction="bh")
        ref = ABTest.from_stream(stream, DIFF, METRICS, correction="bh").fit()
        pd.testing.assert_frame_equal(out, ref)
        assert set(out.columns) <= set(COLUMNS)

    def test_keyword_only_options(self) -> None:
        signature = inspect.signature(Eventstream.get_ab_test)
        assert signature.parameters["assignment"].kind is inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters["correction"].default == "holm"
        with pytest.raises(TypeError):
            _stream().get_ab_test(DIFF, METRICS, None)

    def test_docstring_lists_every_column_and_enum(self) -> None:
        doc = inspect.getdoc(Eventstream.get_ab_test)
        for column in COLUMNS:
            assert f"`{column}`" in doc, column
        for value in (
            "holm",
            "bonferroni",
            "bh",
            "none",
            "z_test",
            "welch_t_test",
            "bootstrap",
            "multinomial",
            "poisson",
            "since_assignment",
            "calendar",
            "itt",
            "active",
            "insufficient_data",
            "descriptive",
        ):
            assert f'"{value}"' in doc, value


class TestGetAbBootstrap:
    def test_long_table(self) -> None:
        table = _stream().get_ab_bootstrap(DIFF, METRICS, n_bootstrap=150, period="5D")
        assert list(table.columns) == [
            "replicate",
            "metric",
            "period",
            "group1_value",
            "group2_value",
            "delta",
        ]
        assert len(table) == 150 * 2

    def test_replicates_validated_as_in_get_ab_test(self) -> None:
        table = _stream().get_ab_bootstrap(DIFF, METRICS, n_bootstrap="auto")
        assert table["replicate"].nunique() == 1000
        for bad in (1, 99, "many", True):
            with pytest.raises(InvalidParameterError):
                _stream().get_ab_bootstrap(DIFF, METRICS, n_bootstrap=bad)


class TestGetMetricsName:
    """U9: get_metrics honours `name` as get_ab_test does."""

    def test_name_labels_the_column(self) -> None:
        stream = _stream()
        out = stream.get_metrics(
            [
                {
                    "name": "conversion",
                    "metric": "has_event",
                    "metric_args": {"event": "order"},
                },
                {"metric": "length"},
            ]
        )
        assert list(out.columns) == ["conversion", "length"]
        plain = stream.get_metrics(
            [{"metric": "has_event", "metric_args": {"event": "order"}}]
        )
        assert (out["conversion"] == plain.iloc[:, 0]).all()

    def test_bad_names_raise(self) -> None:
        from retentioneering.exceptions import InvalidMetricConfigError

        stream = _stream()
        with pytest.raises(InvalidMetricConfigError, match="duplicate"):
            stream.get_metrics([{"name": "a", "metric": "length"}] * 2)
        # m6: a name that repeats another metric's column name
        with pytest.raises(InvalidMetricConfigError, match="duplicate output"):
            stream.get_metrics(
                [{"name": "length", "metric": "duration"}, {"metric": "length"}]
            )
        with pytest.raises(InvalidMetricConfigError, match="single column"):
            stream.get_metrics(
                [{"name": "flags", "metric": "has_event_bulk", "metric_args": {}}]
            )
