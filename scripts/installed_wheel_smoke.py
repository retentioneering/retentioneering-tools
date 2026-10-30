#!/usr/bin/env python3
"""Independent tiny assigned-user arithmetic and real exports, from an installed wheel.

Run through verify_wheel.py, which copies this script outside the checkout and
uses a fresh interpreter with -I. The handwritten aggregates include silent
assignments and distinguish a ratio of sums from a mean of individual ratios.
This smoke does not certify confidence-interval coverage or every API branch.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import sys
from pathlib import Path

os.environ["RETENTIONEERING_NO_TRACK"] = "1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    import pandas as pd

    import retentioneering
    from retentioneering import Eventstream

    package = Path(retentioneering.__file__).resolve().parent
    if not package.is_relative_to(Path(sys.prefix).resolve()):
        raise AssertionError(f"package is outside the isolated environment: {package}")
    distribution = importlib.metadata.distribution("retentioneering")
    assert distribution.version == manifest["version"] == retentioneering.__version__
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    assert not direct_url.get("dir_info", {}).get("editable", False)
    for name, wanted in manifest["source_package_sha256"].items():
        path = package.parent / name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == wanted, name

    # 4 assigned users per arm, including c3/c4/t4 who have no events.
    events = [
        ("c1", "purchase", 10),
        ("c2", "view", 0),
        ("t1", "purchase", 20),
        ("t1", "purchase", 10),
        ("t2", "purchase", 5),
        ("t3", "view", 0),
    ]
    frame = pd.DataFrame(events, columns=["user_id", "event", "amount"])
    frame["arm"] = frame.user_id.str[0]
    frame["timestamp"] = pd.Timestamp("2026-01-03") + pd.to_timedelta(
        range(len(frame)), unit="s"
    )
    stream = Eventstream(
        frame, schema={"segment_cols": ["arm"], "custom_cols": ["amount"]}
    )
    roster = pd.DataFrame(
        [(f"{arm}{i}", arm) for arm in "ct" for i in range(1, 5)],
        columns=["user_id", "arm"],
    )
    roster["assigned_at"] = pd.Timestamp("2026-01-01")
    binary = {"metric": "has_event", "metric_args": {"event": "purchase"}}
    revenue = {
        "metric": "event_value_sum",
        "metric_args": {"event": "purchase", "value_col": "amount"},
    }
    count = {"metric": "event_count", "metric_args": {"event": "purchase"}}
    metrics = [
        {"name": "binary", **binary},
        {"name": "mean", **revenue},
        {"name": "ratio", **revenue, "denominator": count},
    ]
    flexible = {
        "assignment": roster.set_index("user_id"),
        "observation_end": "2026-01-05",
        "random_state": 109,
    }
    scorecard = stream.ab_test_data(("arm", "t", "c"), metrics, **flexible)
    assert scorecard.equals(stream.get_ab_test(("arm", "t", "c"), metrics, **flexible))
    strict_options = {
        "assignments": roster,
        "metrics": [
            {"name": "binary", **binary, "agg": "mean"},
            {"name": "mean", **revenue, "agg": "mean"},
            {
                "name": "ratio",
                "agg": "ratio_of_sums",
                "numerator": revenue,
                "denominator": count,
            },
        ],
        "observation": {
            "start": "2026-01-02",
            "end": "2026-01-05",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        "variant_col": "arm",
        "control": "c",
        "treatment": "t",
    }
    strict = stream.experiment_data(**strict_options)
    expected = {
        "binary": (1 / 4, 2 / 4, 1 / 4),
        "mean": (10 / 4, 35 / 4, 25 / 4),
        "ratio": (10, 35 / 3, 5 / 3),
    }
    for rows, fields in (
        (scorecard.to_dict("records"), ("group2_value", "group1_value", "delta")),
        (strict["summary"], ("control", "treatment", "absolute_effect")),
    ):
        for row in rows:
            for key, wanted in zip(fields, expected[row["metric"]]):
                assert math.isclose(row[key], wanted, rel_tol=1e-12, abs_tol=1e-12), (
                    row["metric"],
                    key,
                    row[key],
                    wanted,
                )
    assert list(scorecard.group1_paths) == list(scorecard.group2_paths) == [4, 4, 4]
    assert sorted(row["n_assigned"] for row in strict["diagnostics"]) == [4, 4]
    assert sorted(row["n_without_events"] for row in strict["diagnostics"]) == [1, 2]
    args.output.mkdir(parents=True, exist_ok=True)
    widgets = {
        "ab_test": stream.ab_test(("arm", "t", "c"), metrics, **flexible),
        "experiment": stream.experiment(**strict_options),
    }
    exports = {}
    for name, widget in widgets.items():
        assert not widget.error, widget.error
        path = args.output / f"{name}.html"
        widget.export_html(str(path))
        text = path.read_text()
        assert name in text and len(text) > 10_000
        exports[name] = {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    result = {
        "package_file": str(package / "__init__.py"),
        "python": sys.version,
        "executable": sys.executable,
        "cwd": os.getcwd(),
        "isolated": bool(sys.flags.isolated),
        "wheel_sha256": manifest["wheel_sha256"],
        "version": distribution.version,
        "direct_url": direct_url,
        "expected_aggregates": expected,
        "exports": exports,
        "dependency_versions": {
            d.metadata["Name"]: d.version for d in importlib.metadata.distributions()
        },
        "point_and_population_checks": "passed",
    }
    (args.output / "installed-wheel-smoke.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: result[k]
                for k in [
                    "package_file",
                    "version",
                    "isolated",
                    "point_and_population_checks",
                ]
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
