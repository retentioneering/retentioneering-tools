"""Tests for the DuckDB instance that `engine.run()` shares.

`run()` takes a cursor on one instance that the whole process shares, instead
of building a new database for every call. This is only safe because cursors
stay separate from each other, so that is tested here, together with the late
build, the lock that protects it, and the fork handler that stops a child
process from using the instance of its parent.
"""

import os
import signal
import subprocess
import sys
import threading
import time

import duckdb
import pandas as pd
import pytest

from retentioneering import engine

THREADS = 16
TIMEOUT = 30


@pytest.fixture()
def fresh_engine():
    """Empty the shared state so a test can watch the instance being built from
    a known start, then put back what the rest of the suite was using."""
    saved_root = engine._ROOT
    saved_abandoned = list(engine._ABANDONED)
    engine._ROOT = None
    engine._ABANDONED.clear()
    try:
        yield
    finally:
        if engine._ROOT is not None:
            engine._ROOT.close()
        engine._ROOT = saved_root
        engine._ABANDONED[:] = saved_abandoned


class TestCursorIsolation:
    """A frame that one call registers must stay hidden from every other call,
    and must not live longer than its own cursor. Sharing one instance is only
    safe because of this."""

    def test__two_cursors_register_the_same_name_without_seeing_each_other(self):
        root = engine._root()
        first, second = root.cursor(), root.cursor()
        try:
            first.register("t", pd.DataFrame({"x": [1]}))
            second.register("t", pd.DataFrame({"x": [2]}))

            assert int(first.sql("SELECT x FROM t").df()["x"][0]) == 1
            assert int(second.sql("SELECT x FROM t").df()["x"][0]) == 2
        finally:
            first.close()
            second.close()

    def test__a_registration_does_not_outlive_its_cursor(self):
        root = engine._root()
        cur = root.cursor()
        cur.register("t", pd.DataFrame({"x": [1]}))
        cur.close()

        later = root.cursor()
        try:
            with pytest.raises(duckdb.Error):
                later.sql("SELECT x FROM t").df()
        finally:
            later.close()

    def test__consecutive_runs_reusing_a_name_each_see_their_own_frame(self):
        first = engine.run("SELECT x FROM t", t=pd.DataFrame({"x": [1]}))
        second = engine.run("SELECT x FROM t", t=pd.DataFrame({"x": [2]}))

        assert int(first["x"][0]) == 1
        assert int(second["x"][0]) == 2

    def test__a_name_from_an_earlier_run_is_gone_in_a_later_one(self):
        engine.run("SELECT x FROM t", t=pd.DataFrame({"x": [1]}))

        with pytest.raises(duckdb.Error):
            engine.run("SELECT x FROM t")

    def test__a_registered_frame_is_never_hidden_by_a_stored_object(self):
        # The shared instance keeps a catalog that lives longer than one call,
        # so a stored table can end up with the same name as a frame that a
        # later call registers. The registered frame has to win. If the stored
        # table won, that later call would quietly read the wrong data. Storing
        # a table is not part of the `sql=` contract, which asks for a SELECT,
        # but a user can still send one.
        root = engine._root()
        setup = root.cursor()
        try:
            setup.execute("CREATE OR REPLACE TABLE shadow_probe AS SELECT 999 AS x")
        finally:
            setup.close()

        try:
            result = engine.run(
                "SELECT count(*) AS n, max(x) AS mx FROM shadow_probe",
                shadow_probe=pd.DataFrame({"x": [1, 1, 1]}),
            )

            assert int(result["n"][0]) == 3
            assert int(result["mx"][0]) == 1
        finally:
            cleanup = root.cursor()
            try:
                cleanup.execute("DROP TABLE IF EXISTS shadow_probe")
            finally:
                cleanup.close()

    def test__every_frame_of_a_multi_table_call_is_registered(self):
        orders = pd.DataFrame({"k": [1, 2, 3]})
        items = pd.DataFrame({"k": [2, 3, 4]})

        result = engine.run(
            "SELECT count(*) AS n FROM orders JOIN items USING (k)",
            orders=orders,
            items=items,
        )

        assert int(result["n"][0]) == 2


class TestLazyInstance:
    def test__importing_the_package_does_not_build_an_instance(self):
        # This is why we build late. A program that imports retentioneering but
        # never runs a query should not pay for an instance it does not use.
        code = (
            "import retentioneering\n"
            "from retentioneering import engine\n"
            "raise SystemExit(0 if engine._ROOT is None else 1)\n"
        )

        assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0

    def test__the_first_query_builds_the_instance(self, fresh_engine):
        assert engine._ROOT is None

        engine.run("SELECT 1 AS x")

        assert engine._ROOT is not None

    def test__later_queries_reuse_the_same_instance(self, fresh_engine):
        engine.run("SELECT 1 AS x")
        built = engine._ROOT

        engine.run("SELECT 2 AS x")

        assert engine._ROOT is built


class TestConcurrency:
    def test__threads_racing_on_the_first_query_build_one_instance(
        self, fresh_engine, monkeypatch
    ):
        built = []
        real_connect = duckdb.connect

        def counting_connect(*args, **kwargs):
            con = real_connect(*args, **kwargs)
            built.append(con)
            return con

        monkeypatch.setattr(duckdb, "connect", counting_connect)

        start = threading.Barrier(THREADS)
        errors = []

        def worker():
            try:
                start.wait(timeout=TIMEOUT)
                engine.run("SELECT 1 AS x")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=TIMEOUT)

        assert errors == []
        assert len(built) == 1

    def test__concurrent_queries_do_not_see_each_others_registrations(self):
        start = threading.Barrier(THREADS)
        seen = {}
        errors = []

        def worker(value):
            try:
                start.wait(timeout=TIMEOUT)
                result = engine.run("SELECT x FROM t", t=pd.DataFrame({"x": [value]}))
                seen[value] = int(result["x"][0])
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(value,)) for value in range(THREADS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=TIMEOUT)

        assert errors == []
        assert seen == {value: value for value in range(THREADS)}


class TestForkHandling:
    def test__the_handler_parks_the_inherited_instance_instead_of_closing_it(
        self, fresh_engine
    ):
        engine.run("SELECT 1 AS x")
        inherited = engine._ROOT

        engine._reset_after_fork()

        assert engine._ROOT is None
        assert engine._ABANDONED == [inherited]
        # It is still open. The handler exists so that an instance from the
        # parent is never closed, so keeping it must not have closed it.
        assert int(inherited.sql("SELECT 1 AS x").df()["x"][0]) == 1

    def test__the_handler_replaces_the_lock(self, fresh_engine):
        # Only the thread that calls fork() lives on in the child, so a lock
        # that another thread held at that moment would stay locked for ever.
        before = engine._ROOT_LOCK

        engine._reset_after_fork()

        assert engine._ROOT_LOCK is not before
        assert not engine._ROOT_LOCK.locked()

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is POSIX only")
    # A DuckDB instance runs threads of its own, so this forks a process that
    # has more than one thread. That is on purpose, because it is the case the
    # handler is there for, and Python prints a warning about it. The wait
    # below has a time limit, so a child that gets stuck fails the test instead
    # of blocking the whole run.
    @pytest.mark.filterwarnings("ignore:This process .* is multi-threaded")
    def test__a_forked_child_rebuilds_its_own_instance_and_queries(self, fresh_engine):
        engine.run("SELECT 1 AS x")  # the instance the child will get

        pid = os.fork()
        if pid == 0:
            # This is the child. It reports what it found through the exit code
            # and leaves with os._exit, so that no pytest cleanup and no atexit
            # handler runs here.
            code = 0
            try:
                if engine._ROOT is not None:
                    code = 10  # the handler did not empty the slot
                elif len(engine._ABANDONED) != 1:
                    code = 11  # the handler did not keep the old instance
                else:
                    result = engine.run("SELECT 42 AS x")
                    if int(result["x"][0]) != 42:
                        code = 12  # the child could not run a query
                    elif engine._ROOT is None:
                        code = 13  # the child did not build its own instance
            except BaseException:
                code = 14
            os._exit(code)

        deadline = time.monotonic() + TIMEOUT
        while True:
            reaped, status = os.waitpid(pid, os.WNOHANG)
            if reaped == pid:
                break
            if time.monotonic() > deadline:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                pytest.fail(f"forked child did not finish within {TIMEOUT}s")
            time.sleep(0.01)

        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
        # The parent still works after the child has come and gone.
        assert int(engine.run("SELECT 7 AS x")["x"][0]) == 7
