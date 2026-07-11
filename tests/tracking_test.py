"""Tests for the PostHog tracking opt-out."""

import json
from functools import cached_property
from unittest.mock import MagicMock

from retentioneering import _tracking
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import EmptyEventstreamError


def test_identify_respects_no_track(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")

    _tracking.identify({"email": "user@example.com"})

    fake_ph.identify.assert_not_called()


def test_identify_sends_when_tracking_enabled(monkeypatch, tmp_path):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")

    _tracking.identify({"email": "user@example.com"})

    fake_ph.identify.assert_called_once_with(
        _tracking._DISTINCT_ID, properties={"email": "user@example.com"}
    )


def test_identify_never_raises(monkeypatch, tmp_path):
    fake_ph = MagicMock()
    fake_ph.identify.side_effect = RuntimeError("network down")
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")

    _tracking.identify({"email": "user@example.com"})  # must not raise


def test_track_respects_no_track(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")

    _tracking.track("some_event", {"key": "value"})

    fake_ph.capture.assert_not_called()


def test_track_respects_no_track_in_config_file(monkeypatch, tmp_path):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    config = tmp_path / "config.json"
    config.write_text('{"RETENTIONEERING_NO_TRACK": true}')
    monkeypatch.setattr(_tracking, "_CONFIG", config)

    _tracking.track("some_event", {"key": "value"})

    fake_ph.capture.assert_not_called()


# ── @tracked: success/error status, changed args, opt-out ──────────────────────


def test_tracked_reports_success_status_and_arg_split(monkeypatch, tmp_path):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")

    class Dummy:
        @_tracking.tracked("dummy_event")
        def method(self, keep=None, drop=None):
            return "ok"

    assert Dummy().method(keep="x") == "ok"

    _, kwargs = fake_ph.capture.call_args
    props = kwargs["properties"]
    assert props["status"] == "success"
    assert props["non_default_args"] == ["keep"]
    assert props["default_args"] == ["drop"]


def test_tracked_reports_error_status_and_reraises(monkeypatch, tmp_path):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")

    class Dummy:
        @_tracking.tracked("dummy_event")
        def method(self, keep=None):
            raise ValueError(f"bad column: {keep}")

    dummy = Dummy()
    try:
        dummy.method(keep="secret_column_name")
        raised = False
    except ValueError:
        raised = True

    assert raised

    _, kwargs = fake_ph.capture.call_args
    props = kwargs["properties"]
    assert props["status"] == "error"
    assert props["error_type"] == "ValueError"
    assert props["non_default_args"] == ["keep"]
    # never leak the exception message (it echoed the arg value above)
    assert "secret_column_name" not in json.dumps(props)


def test_tracked_error_uses_error_code_for_retentioneering_errors(
    monkeypatch, tmp_path
):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")

    class Dummy:
        @_tracking.tracked("dummy_event")
        def method(self):
            raise EmptyEventstreamError("some context")

    try:
        Dummy().method()
    except EmptyEventstreamError:
        pass

    _, kwargs = fake_ph.capture.call_args
    assert kwargs["properties"]["error_type"] == "EMPTY_EVENTSTREAM"


def test_tracked_error_respects_no_track(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")

    class Dummy:
        @_tracking.tracked("dummy_event")
        def method(self):
            raise RuntimeError("boom")

    try:
        Dummy().method()
    except RuntimeError:
        pass

    fake_ph.capture.assert_not_called()


# ── coverage: every public Eventstream method is tracked or explicitly excused ──

# Read-only accessors/inspectors — not "actions" in the dp_/widget_/headless_ sense,
# so they were never wired to @_tracked.
_NOT_TRACKED_BY_DESIGN = {
    "schema",
    "df",
    "fingerprint",
    "to_dataframe",
    "is_empty",
    "equals",
    "get_event_counts",
    "get_segment_values",
}


def _tracked_event_name(member) -> str | None:
    if isinstance(member, (property, cached_property)):
        member = member.fget
    return getattr(member, "_tracked_event", None)


def test_every_public_eventstream_method_is_tracked_or_excluded():
    untracked = sorted(
        name
        for name, member in vars(Eventstream).items()
        if not name.startswith("_")
        and name not in _NOT_TRACKED_BY_DESIGN
        and _tracked_event_name(member) is None
    )

    assert untracked == [], (
        f"Public Eventstream method(s) {untracked} have no @_tracked decorator and "
        "aren't listed in _NOT_TRACKED_BY_DESIGN. Either add @_tracked(...) or, if "
        "tracking genuinely doesn't apply, add the method to the exclusion list with "
        "a comment explaining why."
    )


def test_not_tracked_by_design_methods_still_exist():
    stale = sorted(
        name for name in _NOT_TRACKED_BY_DESIGN if name not in vars(Eventstream)
    )

    assert stale == [], (
        f"_NOT_TRACKED_BY_DESIGN references method(s) {stale} that no longer exist "
        "on Eventstream — remove them from the exclusion list."
    )
