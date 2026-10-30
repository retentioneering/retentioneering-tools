"""Public-result plotting and reproducible tutorial data regression checks."""

import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks"))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from retentioneering import Eventstream

from experiment_demo import CATALOG, OBSERVATION, SCHEMA, make_demo_dataset
from experiment_helpers import result_from_payload
from experiment_plots import plot_effects, plot_funnel
from lumeo_data import load_lumeo_tables

ROOT = Path(__file__).resolve().parents[2]


def _plot_payload(kind):
    users = ["c0", "c1", "c2", "c3", "t0", "t1", "t2", "t3"]
    events = [(user, "open", "2026-01-03 00:00") for user in users]
    if kind in {"finite", "mixed", "no_intervals"}:
        events += [(user, "buy", "2026-01-03 00:01") for user in users[::2]]
    stream = Eventstream(
        pd.DataFrame(events, columns=["user_id", "event", "timestamp"])
    )
    roster = pd.DataFrame(
        {
            "user_id": users,
            "variant": ["c"] * 4 + ["t"] * 4,
            "assigned_at": "2026-01-01",
        }
    )
    metrics = [
        {
            "name": "purchase",
            "metric": "has_event",
            "metric_args": {"event": "buy"},
            "agg": "mean",
        }
    ]
    if kind == "mixed":
        metrics.append(
            {
                "name": "absent",
                "metric": "has_event",
                "metric_args": {"event": "absent"},
                "agg": "mean",
            }
        )
    if kind == "undefined":
        metrics = [
            {
                "name": "purchase",
                "agg": "ratio_of_sums",
                "numerator": {"metric": "event_count", "metric_args": {"event": "buy"}},
                "denominator": {
                    "metric": "event_count",
                    "metric_args": {"event": "buy"},
                },
            }
        ]
    return stream.experiment_data(
        assignments=roster,
        variant_col="variant",
        control="c",
        treatment="t",
        metrics=metrics,
        observation={
            "start": "2026-01-02",
            "end": "2026-01-04",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        event_catalog={"event": ["buy", "absent"]},
        funnel_steps=["buy", "buy"],
        bootstrap=None
        if kind == "no_intervals"
        else {"n_resamples": 199, "random_state": 42},
    )


@pytest.mark.parametrize(
    "kind", ["finite", "mixed", "no_intervals", "all_zero", "undefined"]
)
def test_saved_result_figures_handle_nulls_without_mutating_payload(kind, tmp_path):
    payload = _plot_payload(kind)
    before = copy.deepcopy(payload)
    view = result_from_payload(payload)
    summary_before = view.summary.copy(deep=True)
    metrics = [
        row["metric"] for row in payload["summary"] if row["family"] == "scorecard"
    ]
    figures = [
        plot_effects(
            view, metrics, metrics, ["pp"] * len(metrics), [100] * len(metrics)
        ),
        plot_funnel(view),
        plot_funnel(view, family="funnel_conditional"),
    ]
    try:
        for index, figure in enumerate(figures):
            for extension in ("png", "svg"):
                destination = tmp_path / f"{index}.{extension}"
                figure.savefig(destination)
                assert destination.stat().st_size > 0
        if kind in {"all_zero", "undefined", "no_intervals", "mixed"}:
            texts = " ".join(
                text.get_text()
                for figure in figures
                for ax in figure.axes
                for text in ax.texts
            )
            assert "CI" in texts
            assert (
                "not_requested" in texts
                if kind == "no_intervals"
                else "degenerate" in texts or "undefined" in texts
            )
        if kind == "undefined":
            assert "estimate unavailable" in figures[0].axes[0].get_title(loc="left")
            # Only the vertical zero reference remains: no point is fabricated.
            assert not any(
                line.get_marker() == "o" for line in figures[0].axes[0].lines
            )
        if kind == "all_zero":
            # The zero point is retained even though its CI is unavailable.
            assert any(line.get_marker() == "o" for line in figures[0].axes[0].lines)
    finally:
        for figure in figures:
            plt.close(figure)
    assert payload == before
    pd.testing.assert_frame_equal(view.summary, summary_before)


def test_default_lumeo_fixture_is_independent_of_working_directory(
    monkeypatch, tmp_path
):
    expected = load_lumeo_tables(ROOT / "notebooks/data/lumeo")
    for directory in (ROOT, tmp_path):
        monkeypatch.chdir(directory)
        actual = load_lumeo_tables()
        assert actual["metadata"] == expected["metadata"]
        for name in ("events", "assignments", "screens", "clicks", "layout"):
            pd.testing.assert_frame_equal(actual[name], expected[name])


def test_demo_history_precedes_assignment_and_is_excluded_from_outcomes():
    demo = make_demo_dataset(n_per_arm=12)
    history = demo["events"].loc[lambda frame: frame.event.eq("history_view")]
    assigned_at = demo["assignments"].set_index("user_id").assigned_at
    assert history.timestamp.lt(history.user_id.map(assigned_at)).all()
    result = Eventstream(demo["events"], schema=SCHEMA).experiment_data(
        assignments=demo["assignments"],
        variant_col="variant",
        observation=OBSERVATION,
        event_catalog=CATALOG,
        metrics=[
            {
                "name": "baseline",
                "metric": "event_count",
                "metric_args": {"event": "history_view"},
                "agg": "mean",
            }
        ],
    )
    row = result["summary"][0]
    assert row["control"] == row["treatment"] == 0
    assert row["control_units"] == row["treatment_units"] == 12


def _parent_fixture(path):
    path.mkdir()
    tables = {
        "lumeo_events.csv.gz": pd.DataFrame(
            {"user_id": [1, 1, 2, 3], "event": ["a", "b", "a", "a"]}
        ),
        "lumeo_ab_groups.csv": pd.DataFrame(
            {"user_id": [1, 2, 3], "exp_group": ["control", "treatment", "control"]}
        ),
        "lumeo_ui_screens.csv": pd.DataFrame(
            {"user_id": [1, 2, 3], "screen": ["a"] * 3}
        ),
        "lumeo_ui_clicks.csv": pd.DataFrame({"user_id": [1, 1, 3], "click": [1, 2, 1]}),
        "lumeo_ui_layout.csv": pd.DataFrame({"zone": ["a", "b"]}),
    }
    files = {}
    for name, frame in tables.items():
        frame.to_csv(path / name, index=False)
        files[name] = {
            "sha256_file": hashlib.sha256((path / name).read_bytes()).hexdigest()
        }
    receipt = {
        "dataset_version": "test-v8",
        "seed": 5044,
        "time_basis": "synthetic",
        "event_window": ["2026-01-01", "2026-02-01"],
        "click_window_from": "2026-01-02",
        "files": files,
    }
    (path / "build_receipt.json").write_text(json.dumps(receipt))
    return tables


def _build_sample(source, output):
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "examples/experiments/build_lumeo_sample.py"),
            str(source),
            "--output",
            str(output),
            "--n-users",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_documented_builder_preserves_sources_and_reproduces_sample(tmp_path):
    source = tmp_path / "parent"
    tables = _parent_fixture(source)
    initial = {path.name: path.read_bytes() for path in source.iterdir()}
    outputs = [tmp_path / "sample-a", tmp_path / "sample-b"]
    manifests = []
    for output in outputs:
        run = _build_sample(source, output)
        assert run.returncode == 0, run.stderr
        manifests.append(json.loads((output / "manifest.json").read_text()))
    assert manifests[0] == manifests[1]
    frozen = json.loads((ROOT / "notebooks/data/lumeo/manifest.json").read_text())
    assert manifests[0]["license_status"] == frozen["license_status"]
    assert manifests[0]["serialization"]["pandas"] == pd.__version__
    selected = set(pd.read_csv(outputs[0] / "assignments.csv.gz").user_id)
    expected_users = sorted(
        [1, 2, 3],
        key=lambda user: (
            hashlib.sha256(
                f"lumeo-ab-tutorial-2026-10-29-v1|{user}".encode()
            ).hexdigest(),
            user,
        ),
    )[:2]
    assert selected == set(expected_users)
    for info in manifests[0]["files"].values():
        first, second = (output / info["path"] for output in outputs)
        assert first.read_bytes() == second.read_bytes()
        assert hashlib.sha256(first.read_bytes()).hexdigest() == info["sha256"]
        frame = pd.read_csv(first)
        if "user_id" in frame:
            assert set(frame.user_id).issubset(selected)
        else:
            pd.testing.assert_frame_equal(frame, tables["lumeo_ui_layout.csv"])
    assert {path.name: path.read_bytes() for path in source.iterdir()} == initial
    repeated = _build_sample(source, outputs[0])
    assert repeated.returncode != 0
    assert "Output must be empty" in repeated.stderr
    assert {path.name: path.read_bytes() for path in source.iterdir()} == initial


def test_builder_rejects_receipt_mismatch_before_writing_tables(tmp_path):
    source = tmp_path / "parent"
    _parent_fixture(source)
    with (source / "lumeo_ui_layout.csv").open("a") as changed:
        changed.write("changed\n")
    output = tmp_path / "sample"
    run = _build_sample(source, output)
    assert run.returncode != 0
    assert "Source receipt mismatch" in run.stderr
    assert not list(output.iterdir())


def test_documented_builder_command_exists_and_has_help():
    card = (ROOT / "notebooks/data/lumeo/DATA_CARD.md").read_text()
    command = "examples/experiments/build_lumeo_sample.py"
    assert command in card
    run = subprocess.run(
        [sys.executable, str(ROOT / command), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0
    assert "--n-users" in run.stdout
