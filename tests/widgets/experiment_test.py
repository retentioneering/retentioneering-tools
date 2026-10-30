"""Experiment widget calculation, snapshot and static-export contracts."""

import inspect
import json
import re

import pandas as pd
import pytest
from traitlets import TraitError

from retentioneering import Eventstream
from retentioneering.widgets import _html_export
from retentioneering.widgets.experiment import ExperimentWidget


@pytest.fixture
def experiment_case():
    stream = Eventstream(
        pd.DataFrame(
            {
                "user_id": [
                    "private_c1",
                    "private_c1",
                    "private_c2",
                    "private_t1",
                    "private_t1",
                    "private_t2",
                ],
                "event": ["open", "buy", "open", "open", "buy", "open"],
                "timestamp": pd.date_range("2026-01-03", periods=6, freq="1min"),
            }
        )
    )
    assignments = pd.DataFrame(
        {
            "user_id": [f"private_{arm}{i}" for arm in "ct" for i in range(1, 4)],
            "variant": ["control"] * 3 + ["treatment"] * 3,
            "assigned_at": pd.Timestamp("2026-01-02"),
        }
    )
    params = {
        "assignments": assignments,
        "variant_col": "variant",
        "metrics": [
            {
                "name": "purchase",
                "metric": "has_event",
                "metric_args": {"event": "buy"},
                "agg": "mean",
            }
        ],
        "observation": {
            "start": "2026-01-03",
            "end": "2026-01-05",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        "bootstrap": {"n_resamples": 199, "random_state": 42},
        "funnel_steps": ["open", "buy"],
    }
    return stream, params


def test_experiment_widget_preserves_headless_payload(experiment_case):
    stream, params = experiment_case
    expected = stream.experiment_data(**params)
    widget = stream.experiment(**params)

    assert isinstance(widget, ExperimentWidget)
    assert json.loads(widget.result) == expected
    assert expected["schema_version"] == "2.0"
    assert widget.error == ""


def test_display_changes_and_exports_do_not_recompute(experiment_case, monkeypatch):
    stream, params = experiment_case
    calls = []
    calculate = stream.experiment_data

    def recorded(**kwargs):
        calls.append(kwargs)
        return calculate(**kwargs)

    monkeypatch.setattr(stream, "experiment_data", recorded)
    widget = ExperimentWidget(stream, **params)
    result = widget.result
    widget.height = 720
    widget.sidebar_open = False
    payload = widget._export_data()

    assert len(calls) == 1
    assert widget.result == result
    assert payload["result"] == json.loads(result)
    assert payload["height"] == 720
    assert payload["sidebar_open"] is False
    assert widget._export_data(sidebar_open=True)["sidebar_open"] is True
    assert widget.sidebar_open is False


def test_assignment_rows_are_not_synced_or_exported(experiment_case):
    stream, params = experiment_case
    widget = ExperimentWidget(stream, **params)
    state = widget.sync_state()

    assert "assignments" not in state
    exported = json.dumps(widget._export_data())
    for user_id in params["assignments"].user_id:
        assert user_id not in exported
        assert user_id not in json.dumps(state)
    assert widget.compute_tools == {}


def test_calculated_result_is_a_read_only_detached_snapshot(
    experiment_case, monkeypatch
):
    stream, params = experiment_case
    original = stream.experiment_data(**params)
    monkeypatch.setattr(stream, "experiment_data", lambda **kwargs: original)
    widget = ExperimentWidget(stream, **params)
    before = widget.result
    original["metadata"]["control"] = "changed after calculation"
    original["summary"][0]["control"] = 999

    assert widget.result == before
    with pytest.raises(TraitError):
        widget.result = "{}"


def test_experiment_html_export_matches_live_result(
    experiment_case, tmp_path, monkeypatch
):
    bundle = tmp_path / "widget-static.js"
    bundle.write_text("/* test bundle */")
    monkeypatch.setattr(_html_export, "_BUNDLE_PATH", bundle)
    stream, params = experiment_case
    widget = ExperimentWidget(stream, **params)
    path = tmp_path / "experiment.html"
    widget.export_html(str(path), title="Experiment")

    match = re.search(
        r"<script>window\.__HS_DATA__ = (.*?);</script>", path.read_text(), re.DOTALL
    )
    assert match is not None
    payload = json.loads(match.group(1))
    assert payload == widget._export_data()
    assert payload["widget_type"] == "experiment"
    assert payload["result"] == stream.experiment_data(**params)
    assert "<iframe srcdoc=" in widget.render_static().data


def test_widget_data_parameters_match_headless():
    headless = inspect.signature(Eventstream.experiment_data).parameters
    widget = inspect.signature(ExperimentWidget.__init__).parameters
    expected = {
        key: (p.kind, p.default) for key, p in headless.items() if key != "self"
    }
    actual = {
        key: (p.kind, p.default)
        for key, p in widget.items()
        if key not in {"self", "eventstream", "height", "sidebar_open"}
    }
    assert actual == expected


def test_state_file_restore_is_not_silently_accepted(experiment_case):
    stream, params = experiment_case
    with pytest.raises(TypeError, match="state_file"):
        ExperimentWidget(stream, **params, state_file="not-supported.json")
