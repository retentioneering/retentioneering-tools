"""Tests for the PostHog tracking opt-out."""

from unittest.mock import MagicMock

from retentioneering import _tracking


def test_identify_respects_no_track(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")

    _tracking.identify({"email": "user@example.com"})

    fake_ph.identify.assert_not_called()


def test_identify_sends_when_tracking_enabled(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)

    _tracking.identify({"email": "user@example.com"})

    fake_ph.identify.assert_called_once_with(
        _tracking._DISTINCT_ID, properties={"email": "user@example.com"}
    )


def test_identify_never_raises(monkeypatch):
    fake_ph = MagicMock()
    fake_ph.identify.side_effect = RuntimeError("network down")
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)

    _tracking.identify({"email": "user@example.com"})  # must not raise


def test_track_respects_no_track(monkeypatch):
    fake_ph = MagicMock()
    monkeypatch.setattr(_tracking, "_ph", fake_ph)
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")

    _tracking.track("some_event", {"key": "value"})

    fake_ph.capture.assert_not_called()
