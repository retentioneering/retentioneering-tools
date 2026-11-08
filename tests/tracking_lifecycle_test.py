"""Client lifecycle regressions; subprocesses isolate imports and SDK threads."""

import multiprocessing
import os
import socket
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


@pytest.mark.parametrize("opt_out", ["environment", "config", "none"])
def test_fresh_import_does_not_load_sdk_or_start_threads(tmp_path, opt_out):
    if opt_out == "config":
        config = tmp_path / ".retentioneering" / "config.json"
        config.parent.mkdir()
        config.write_text('{"RETENTIONEERING_NO_TRACK": true}')
    _run_python(
        f"""
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
        if {opt_out != "none"!r}:
            _tracking.track("blocked")
            _tracking.identify({{}})
        assert "posthog" not in sys.modules
        assert _tracking._ph is None
        assert not _tracking._ph_attempted
        assert not any(name.startswith("posthog") for name in starts), starts
        """,
        tmp_path,
        opt_out=opt_out == "environment",
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
@pytest.mark.parametrize("state", ["unused", "locked", "initializing", "failed"])
def test_fork_child_preserves_attempt_without_parent_lock(tmp_path, state):
    _run_python(
        f"""
        import os
        import sys
        import threading
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from retentioneering import _tracking
        os.environ.pop("RETENTIONEERING_NO_TRACK")
        _tracking._no_track_requested = lambda: False
        constructor = MagicMock()
        sys.modules["posthog"] = SimpleNamespace(Posthog=constructor)
        if {state!r} in ("locked", "initializing"):
            # State of a lock owned by another thread at the instant of fork.
            _tracking._ph_lock.acquire()
        if {state!r} in ("initializing", "failed"):
            _tracking._ph_attempted = True
        pid = os.fork()
        if pid == 0:
            import signal
            signal.alarm(5)
            try:
                _tracking.track("child")
                _tracking.identify({{}})
                if {state!r} in ("initializing", "failed"):
                    constructor.assert_not_called()
                    assert _tracking._ph is None
                else:
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


@pytest.mark.parametrize("method", multiprocessing.get_all_start_methods())
def test_opted_out_workers_do_not_start_posthog(tmp_path, method):
    if method == "forkserver":
        try:
            with socket.socket(socket.AF_UNIX):
                pass
        except PermissionError:
            pytest.skip("environment forbids Unix sockets required by forkserver")
    worker = """
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
_tracking.track("blocked")
_tracking.identify({})
assert "posthog" not in sys.modules
assert _tracking._ph is None
assert not any(name.startswith("posthog") for name in starts), starts
"""
    _run_python(
        f"""
        import multiprocessing
        import retentioneering
        process = multiprocessing.get_context({method!r}).Process(
            target=exec, args=({worker!r},)
        )
        process.start()
        process.join(15)
        if process.is_alive():
            process.terminate()
            process.join(5)
            raise AssertionError("worker did not exit")
        assert process.exitcode == 0, process.exitcode
        """,
        tmp_path,
    )
