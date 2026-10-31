"""The ``ab_compression`` widget: the plan of ``get_ab_compression``, step by step.

Every number of the payload is the twin's (``ab_compression_data``, an alias of
``get_ab_compression``): the path rows are ``attrs["path"]``, and the label of every
event at every step, rebuilt from the payload's ``changes`` as the browser does, is
``attrs["mappings"]`` and, at a step ``k``, the plan of
``get_ab_compression(..., step=k)``. Then the widget mechanics: signatures, the
sidebar's recompute (the screen switch sets the other screen's arguments aside),
errors at construction, ``by`` segments, ``state_file`` and the static export.
"""

from __future__ import annotations

import inspect
import json
import re

import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.widgets.ab_compression import (
    PATH_FIELDS,
    VIEW_DEFAULTS,
    ABCompressionWidget,
    action_of,
)

DIFF = ("arm", "treatment", "control")
EXP = ("exp_group", "treatment", "control")
ARGS = {"keep": ["quick_add", "cart"], "max_events": 10}


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return datasets.load_ab_test()


@pytest.fixture(scope="module")
def assignment() -> pd.DataFrame:
    return datasets.load_ab_test_assignment()


@pytest.fixture(scope="module")
def widget(stream, assignment):
    return stream.ab_compression(DIFF, assignment=assignment, **ARGS)


def _strict(text: str):
    return json.loads(
        text, parse_constant=lambda c: pytest.fail(f"bare {c} in widget JSON")
    )


def _label_at(segment: dict, event: str, step: int):
    """``labelAt`` of ab_compression_encoding.ts: the last change at or before
    ``step``."""
    label = event
    for at, value in segment["changes"].get(event, []):
        if at > step:
            break
        label = value
    return label


class TestTwin:
    def test__ab_compression_data_is_get_ab_compression(
        self, stream, assignment
    ) -> None:
        twin = stream.ab_compression_data(DIFF, "steps", assignment=assignment)
        plan = stream.get_ab_compression(DIFF, "steps", assignment=assignment)
        pd.testing.assert_frame_equal(twin, plan)
        pd.testing.assert_frame_equal(twin.attrs["path"], plan.attrs["path"])
        assert twin.attrs["mappings"] == plan.attrs["mappings"]

    def test__signatures(self) -> None:
        get = inspect.signature(Eventstream.get_ab_compression).parameters
        alias = inspect.signature(Eventstream.ab_compression_data).parameters
        assert list(alias) == list(get)
        assert all(alias[n].default == get[n].default for n in get)
        assert all(alias[n].kind == get[n].kind for n in get)
        params = inspect.signature(Eventstream.ab_compression).parameters
        assert list(params)[: len(get)] == list(get)
        assert list(params)[len(get) :] == ["height", "sidebar_open", "state_file"]
        assert all(params[n].kind == get[n].kind for n in get)

    def test__docstrings_are_built_from_the_getter(self) -> None:
        get = inspect.getdoc(Eventstream.get_ab_compression)
        alias = inspect.getdoc(Eventstream.ab_compression_data)
        draw = inspect.getdoc(Eventstream.ab_compression)
        assert alias.splitlines()[0] == get.splitlines()[0]
        assert "Documented alias of `get_ab_compression`" in alias
        params = get.split("Parameters\n----------\n")[1].split("\n\nReturns")[0]
        assert params in alias and params in draw
        assert "height : int, default 640" in draw and "state_file" in draw
        assert "artificial transitions" in draw and "slider" in draw

    def test__widget_defaults_are_the_getters(self, stream) -> None:
        params = ABCompressionWidget(stream, EXP)._params()
        sig = inspect.signature(Eventstream.get_ab_compression).parameters
        for name, value in params.items():
            if name == "assignment":
                continue
            assert value == sig[name].default, name


class TestPayloadIsThePlan:
    def test__path_rows_are_attrs_path(self, stream, assignment, widget) -> None:
        plan = stream.get_ab_compression(DIFF, assignment=assignment, **ARGS)
        payload = _strict(widget.result)
        (segment,) = payload["segments"]
        assert segment["chosen_step"] == plan.attrs["chosen_step"]
        path = plan.attrs["path"]
        assert len(segment["path"]) == len(path) > 4
        for sent, row in zip(segment["path"], path.to_dict("records")):
            # the users-needed columns come with target_delta only
            assert set(sent) == set(PATH_FIELDS) & set(row)
            assert "untestable_share" in sent
            for field in set(PATH_FIELDS) & set(row):
                value = row[field]
                if isinstance(value, float) and value != value:
                    assert sent[field] is None
                else:
                    assert sent[field] == pytest.approx(value), field
        assert payload["screen"] == "transitions" and payload["unit"] == "pp"

    def test__changes_rebuild_every_mapping(self, stream, assignment, widget) -> None:
        plan = stream.get_ab_compression(DIFF, assignment=assignment, **ARGS)
        (segment,) = _strict(widget.result)["segments"]
        mappings = plan.attrs["mappings"]["all"]
        assert sorted(segment["events"]) == sorted(mappings[0])
        for step, mapping in enumerate(mappings):
            for event, label in mapping.items():
                assert _label_at(segment, event, step) == label, (event, step)

    @pytest.mark.parametrize("step", [0, 2, None])
    def test__the_table_at_a_step_is_the_plan_at_that_step(
        self, stream, assignment, widget, step
    ) -> None:
        (segment,) = _strict(widget.result)["segments"]
        at = segment["chosen_step"] if step is None else step
        plan = stream.get_ab_compression(DIFF, assignment=assignment, step=at, **ARGS)
        for row in plan.to_dict("records"):
            label = _label_at(segment, row["event"], at)
            assert label == row["label"], row["event"]
            assert action_of(row["event"], label, "other") == row["action"]

    def test__kept_reasons_and_shares(self, stream, assignment, widget) -> None:
        plan = stream.get_ab_compression(DIFF, assignment=assignment, **ARGS)
        (segment,) = _strict(widget.result)["segments"]
        shares = dict(zip(segment["events"], segment["path_share"]))
        for row in plan.to_dict("records"):
            assert shares[row["event"]] == pytest.approx(row["path_share"])
        assert "declared in keep" in segment["kept_reason"]["quick_add"]

    def test__segments(self, stream) -> None:
        diff = EXP
        w = stream.ab_compression(diff, by="platform", by_pre_exposure=True)
        payload = _strict(w.result)
        plan = stream.get_ab_compression(diff, by="platform", by_pre_exposure=True)
        keys = [s["key"] for s in payload["segments"]]
        assert keys == list(plan.attrs["mappings"]) and len(keys) > 1
        for s in payload["segments"]:
            assert s["homogeneity"]["segment"] == s["key"]
            assert s["chosen_step"] == plan.attrs["chosen_step"][s["key"]]
            part = plan[plan["platform"].astype(str) == s["key"]]
            assert sorted(s["events"]) == sorted(part["event"])
        # the view picks the segment shown
        w.segment = keys[1]
        assert w.segment == keys[1] and _strict(w.result) == payload


class TestWidget:
    def test__sidebar_changes_recompute(self, stream) -> None:
        w = stream.ab_compression(EXP, max_events=10)
        before = _strict(w.result)
        w.max_events = 8
        after = _strict(w.result)
        assert after["max_events"] == 8 and after != before
        # the screen switch: the transitions' edge_weight waits, steps compute
        w.screen = "steps"
        steps = _strict(w.result)
        assert steps["screen"] == "steps" and steps["max_steps"] == 10
        assert not w.error
        w.screen = "transitions"
        assert _strict(w.result)["screen"] == "transitions"

    def test__a_bad_sidebar_change_shows_an_error(self, stream) -> None:
        w = stream.ab_compression(EXP)
        w.min_path_share = 2.0
        assert "min_path_share" in w.error and w.result == "{}"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_steps": 5},
            {"screen": "steps", "edge_weight": "unique_paths"},
            {"screen": "graph"},
            {"keep": ["nope"]},
            {"target": "fast"},
            {"min_path_share": 2},
        ],
    )
    def test__construction_errors_are_the_getters(self, stream, kwargs) -> None:
        with pytest.raises(InvalidParameterError) as getter:
            stream.get_ab_compression(EXP, **kwargs)
        with pytest.raises(InvalidParameterError) as drawn:
            stream.ab_compression(EXP, **kwargs)
        assert str(drawn.value) == str(getter.value)

    def test__target_and_lists_round_trip(self, stream) -> None:
        w = stream.ab_compression(EXP, target=0.05, keep=["quick_add"], max_events=9)
        params = w._params()
        assert params["target"] == 0.05 and params["keep"] == ["quick_add"]
        assert params["max_events"] == 9 and params["step"] is None

    def test__target_delta_adds_the_users_needed(self, stream, assignment) -> None:
        w = stream.ab_compression(DIFF, assignment=assignment, target_delta=0.02)
        payload = _strict(w.result)
        plan = stream.get_ab_compression(DIFF, assignment=assignment, target_delta=0.02)
        assert payload["target_delta"] == 0.02
        assert payload["target"] == "untestable_share"
        path = plan.attrs["path"]
        for sent, row in zip(payload["segments"][0]["path"], path.to_dict("records")):
            for name in ("median_scale_needed", "scale_to_test_90"):
                assert sent[name] == pytest.approx(row[name], nan_ok=True), name
        # the sidebar clears it (0: none) and the columns go
        w.target_delta = 0.0
        cleared = _strict(w.result)
        assert cleared["target_delta"] is None
        assert "median_scale_needed" not in cleared["segments"][0]["path"][0]
        assert "target_delta" in ABCompressionWidget._editable

    def test__dispatch_compute(self, stream, widget) -> None:
        raw = widget.dispatch_compute("ab_compression_data", {})
        assert _strict(json.dumps(raw, allow_nan=False)) == _strict(widget.result)


class TestStateAndExport:
    def test__state_is_saved_and_restored(self, stream, tmp_path) -> None:
        path = tmp_path / "compression.json"
        first = stream.ab_compression(EXP, max_events=10, state_file=str(path))
        first.selected_step = 2
        first.changed_only = False
        first.chain_threshold = 0.9
        saved = json.loads(path.read_text())
        assert saved["widget_type"] == "ab_compression"
        assert set(VIEW_DEFAULTS) <= set(saved["state"])
        second = stream.ab_compression(EXP, state_file=str(path))
        assert (second.selected_step, second.changed_only) == (2, False)
        assert (second.chain_threshold, second.max_events) == (0.9, 10)
        assert _strict(second.result) == _strict(first.result)

    def test__export_html_and_render_static(self, widget, tmp_path) -> None:
        data = widget._export_data()
        json.dumps(data, allow_nan=False)
        for name in widget._persist_names:
            assert name in data, name
        out = tmp_path / "compression.html"
        widget.export_html(str(out))
        html = out.read_text()
        assert '"widget_type": "ab_compression"' in html
        assert "<title>A/B Compression</title>" in html
        embedded = re.search(r"window.__HS_DATA__ = (.*?);</script>", html)
        page = json.loads(embedded.group(1).replace("<\\/", "</"))
        assert page["result"] == _strict(widget.result)
        assert "srcdoc" in widget.render_static().data
