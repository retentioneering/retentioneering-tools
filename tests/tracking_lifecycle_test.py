"""Client lifecycle regressions; subprocesses isolate imports and SDK threads."""

import os
import subprocess
import sys
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from retentioneering import _tracking


@pytest.fixture
def lazy_client(monkeypatch, tmp_path):
    constructor = MagicMock()
    monkeypatch.setitem(sys.modules, "posthog", SimpleNamespace(Posthog=constructor))
    monkeypatch.setattr(_tracking, "_ph", None)
    monkeypatch.setattr(_tracking, "_ph_attempted", False)
    monkeypatch.setattr(_tracking, "_ph_lock", threading.Lock())
    monkeypatch.setattr(_tracking, "_CONFIG", tmp_path / "config.json")
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK", raising=False)
    return constructor


def _run_python(code, tmp_path, *, opt_out=True):
    env = dict(os.environ, HOME=str(tmp_path), USERPROFILE=str(tmp_path))
    env.pop("RETENTIONEERING_NO_TRACK", None)
    if opt_out:
        env["RETENTIONEERING_NO_TRACK"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_fresh_import_does_not_load_sdk_or_start_threads(tmp_path):
    # Tracking enabled, no event sent: import alone must not build the client.
    # Opt-out before the first call is covered in-process below.
    _run_python(
        """
        import sys
        import threading
        starts = []
        original_start = threading.Thread.start
        def record_start(self, *args, **kwargs):
            starts.append(type(self).__module__)
            return original_start(self, *args, **kwargs)
        threading.Thread.start = record_start
        import retentioneering
        from retentioneering import _tracking
        assert "posthog" not in sys.modules
        assert _tracking._ph is None
        assert not _tracking._ph_attempted
        assert not any(name.startswith("posthog") for name in starts), starts
        """,
        tmp_path,
        opt_out=False,
    )


@pytest.mark.parametrize("first", ["track", "identify"])
def test_first_enabled_call_builds_once(lazy_client, monkeypatch, first):
    monkeypatch.setenv("RETENTIONEERING_NO_TRACK", "1")
    _tracking.track("blocked")
    _tracking.identify({})
    lazy_client.assert_not_called()
    monkeypatch.delenv("RETENTIONEERING_NO_TRACK")
    if first == "track":
        _tracking.track("first")
        _tracking.identify({})
    else:
        _tracking.identify({})
        _tracking.track("first")
    lazy_client.assert_called_once_with(
        project_api_key=_tracking._KEY, host=_tracking._HOST
    )
    assert lazy_client.return_value.capture.call_count == 1
    assert lazy_client.return_value.identify.call_count == 1


@pytest.mark.parametrize("first", ["track", "identify"])
def test_concurrent_first_calls_wait_for_same_client(lazy_client, monkeypatch, first):
    started = threading.Event()
    release = threading.Event()
    second_progress = threading.Event()
    second_done = threading.Event()
    lock = threading.Lock()
    client = MagicMock()

    class ObservedLock:
        def __enter__(self):
            if started.is_set():
                second_progress.set()
            lock.acquire()

        def __exit__(self, *args):
            lock.release()

    def construct(**kwargs):
        started.set()
        assert release.wait(5), "constructor was never released"
        return client

    def call(name, second=False):
        try:
            if name == "track":
                _tracking.track("event")
            else:
                _tracking.identify({})
        finally:
            if second:
                second_done.set()
                second_progress.set()

    lazy_client.side_effect = construct
    monkeypatch.setattr(_tracking, "_ph_lock", ObservedLock())
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(call, first)
        try:
            assert started.wait(5)
            two = pool.submit(call, "identify" if first == "track" else "track", True)
            assert second_progress.wait(5)
            assert not second_done.is_set(), "event dropped during initialization"
        finally:
            release.set()
        one.result(timeout=5)
        two.result(timeout=5)
    lazy_client.assert_called_once()
    assert client.capture.call_count == 1
    assert client.identify.call_count == 1


def test_failed_initialization_is_not_retried(lazy_client):
    lazy_client.side_effect = RuntimeError("unavailable")
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: _tracking.track("event"), range(16)))
    _tracking.identify({})
    lazy_client.assert_called_once()
    assert _tracking._ph is None


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is POSIX only")
def test_fork_child_does_not_inherit_held_lock(tmp_path):
    _run_python(
        """
        import os
        import sys
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from retentioneering import _tracking
        os.environ.pop("RETENTIONEERING_NO_TRACK")
        _tracking._no_track_requested = lambda: False
        constructor = MagicMock()
        sys.modules["posthog"] = SimpleNamespace(Posthog=constructor)
        # State of a lock owned by another thread at the instant of fork.
        _tracking._ph_lock.acquire()
        pid = os.fork()
        if pid == 0:
            import signal
            signal.alarm(5)
            try:
                _tracking.track("child")
                _tracking.identify({})
                constructor.assert_called_once()
                assert constructor.return_value.capture.call_count == 1
                assert constructor.return_value.identify.call_count == 1
            except BaseException:
                os._exit(1)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0, status
        constructor.assert_not_called()
        """,
        tmp_path,
    )


def test_real_sdk_client_thread_and_runtime_opt_out(tmp_path):
    _run_python(
        """
        import os
        import socket
        from unittest.mock import patch
        from retentioneering import _tracking
        import posthog
        from posthog.consumer import Consumer

        def no_network(*args, **kwargs):
            raise AssertionError("unexpected network access")

        socket.socket.connect = no_network
        sent = []
        # Keep the actual SDK constructor, queue and consumer thread. Replace
        # only transport so this test cannot send telemetry.
        Consumer.request = lambda self, batch: sent.extend(batch)
        original = posthog.Posthog
        posthog.Posthog = lambda **kw: original(
            **kw, flush_at=1, flush_interval=0.01
        )
        os.environ.pop("RETENTIONEERING_NO_TRACK")
        _tracking.track("first")
        client = _tracking._ph
        try:
            assert client is not None
            assert any(t.is_alive() for t in client.consumers)
            _tracking.identify({"test_property": "synthetic"})
            client.flush()
            assert len(sent) == 2, sent
            os.environ["RETENTIONEERING_NO_TRACK"] = "1"
            with patch.object(client, "capture") as capture, patch.object(
                client, "identify" if hasattr(client, "identify") else "set"
            ) as identify:
                _tracking.track("blocked")
                _tracking.identify({})
                capture.assert_not_called()
                identify.assert_not_called()
            # Runtime opt-out gates new calls; it is not SDK shutdown.
            assert any(t.is_alive() for t in client.consumers)
        finally:
            client.join()
        assert not any(t.is_alive() for t in client.consumers)
        """,
        tmp_path,
    )


def test_identify_supports_sdk_set_api(lazy_client):
    client = SimpleNamespace(set=MagicMock())
    lazy_client.return_value = client
    _tracking.identify({"test_property": "synthetic"})
    lazy_client.assert_called_once()
    client.set.assert_called_once_with(
        distinct_id=_tracking._DISTINCT_ID,
        properties={"test_property": "synthetic"},
    )
