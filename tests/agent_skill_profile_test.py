import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("host", [".agents", ".claude"])
def test_empty_export_emits_actionable_profile(host, tmp_path):
    source = tmp_path / "events.csv"
    source.write_text("user_id,event,timestamp\n", encoding="utf-8")
    script = (
        Path(__file__).resolve().parents[1]
        / host
        / "skills/retentioneering-product-analytics/scripts/inspect_event_log.py"
    )
    env = dict(os.environ, RETENTIONEERING_NO_TRACK="1", PYTHONIOENCODING="cp1252")
    result = subprocess.run(
        [sys.executable, str(script), str(source), "--out", str(tmp_path / "out")],
        capture_output=True,
        env=env,
        timeout=30,
    )
    assert result.returncode == 1
    assert b"Traceback" not in result.stderr
    assert b"WARNING:" in result.stdout
    profile = json.loads(
        (tmp_path / "out/data-profile.json").read_text(encoding="utf-8")
    )
    assert profile["rows_read"] == profile["n_paths"] == profile["n_event_types"] == 0
    assert any("no rows" in problem for problem in profile["problems"])
    assert source.read_text(encoding="utf-8") == "user_id,event,timestamp\n"
