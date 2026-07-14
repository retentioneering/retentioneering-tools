"""Verifies js/widget/src/generated/metric_names.generated.ts (the JS metric
editor's METRIC_NAMES) hasn't drifted from metrics/metric_builder.py's
VALID_METRICS — see ADR-0007. If this fails, someone added/renamed/removed a
metric without running `make export-metric-schema`.
"""

import importlib.util
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "export_metric_schema", REPO_ROOT / "scripts" / "export_metric_schema.py"
)
_export_metric_schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_export_metric_schema)


def test_generated_metric_names_is_up_to_date():
    from retentioneering.metrics.metric_builder import VALID_METRICS

    expected = _export_metric_schema.render(sorted(VALID_METRICS))
    actual = _export_metric_schema.OUTPUT_PATH.read_text()

    assert actual == expected, (
        "js/widget/src/generated/metric_names.generated.ts is stale — run "
        "`make export-metric-schema` and commit the result."
    )
