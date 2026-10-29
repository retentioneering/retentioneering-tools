"""Build a deterministic whole-user teaching sample, without altering source data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import platform
import zlib
from pathlib import Path

import numpy as np
import pandas as pd


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("source", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--n-users", type=int, default=4000)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        raise ValueError("Output must be empty; existing data is never overwritten")
    receipt = json.loads((args.source / "build_receipt.json").read_text())
    filenames = {
        "events": "lumeo_events.csv.gz",
        "assignments": "lumeo_ab_groups.csv",
        "screens": "lumeo_ui_screens.csv",
        "clicks": "lumeo_ui_clicks.csv",
        "layout": "lumeo_ui_layout.csv",
    }
    source_hashes = {k: digest(args.source / v) for k, v in filenames.items()}
    for key, name in filenames.items():
        if source_hashes[key] != receipt["files"][name]["sha256_file"]:
            raise ValueError(f"Source receipt mismatch: {name}")
    assignments = pd.read_csv(args.source / filenames["assignments"])
    assert assignments.user_id.is_unique and assignments.user_id.notna().all()
    if not 1 <= args.n_users <= len(assignments):
        raise ValueError("n_users outside source roster size")
    salt = "lumeo-ab-tutorial-2026-10-29-v1"
    ranked = sorted(
        assignments.user_id,
        key=lambda u: (
            hashlib.sha256(f"{salt}|{int(u)}".encode()).hexdigest(),
            int(u),
        ),
    )
    selected = set(ranked[: args.n_users])
    manifest = {
        "dataset_version": "lumeo-ab-sample-1",
        "parent_package": "Lumeo v8.1E3.5",
        "parent_dataset_version": receipt["dataset_version"],
        "generator_seed": receipt["seed"],
        "kind": "selected_synthetic_teaching",
        "selected_parent": True,
        "sampling": {
            "algorithm": "sha256(UTF8(salt+'|'+decimal_user_id)); lexical digest then numeric ID",
            "salt": salt,
            "n_users": args.n_users,
            "outcome_based_selection": False,
        },
        "time_basis": receipt["time_basis"],
        "event_window": receipt["event_window"],
        "click_window_start": receipt["click_window_from"],
        "source_hashes": source_hashes,
        "source_receipt_sha256": digest(args.source / "build_receipt.json"),
        "license_status": "Apache-2.0; synthetic teaching fixture contributed with the Retentioneering tutorial.",
        "serialization": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "zlib": zlib.ZLIB_RUNTIME_VERSION,
            "csv": {"encoding": "utf-8", "index": False, "lineterminator": "\n"},
            "gzip": {"mtime": 0, "filename": "", "compresslevel": 9},
        },
        "files": {},
    }
    for key, name in filenames.items():
        df = pd.read_csv(args.source / name)
        if "user_id" in df:
            df = df.loc[df.user_id.isin(selected)].copy()
        output = args.output / f"{key}.csv.gz"
        # Fix both mtime and embedded filename; retain source row order.
        with (
            output.open("wb") as raw,
            gzip.GzipFile(
                fileobj=raw, filename="", mode="wb", mtime=0, compresslevel=9
            ) as gz,
        ):
            gz.write(df.to_csv(index=False, lineterminator="\n").encode("utf-8"))
        manifest["files"][key] = {
            "path": output.name,
            "rows": len(df),
            "sha256": digest(output),
            "columns": list(df.columns),
        }
    assert source_hashes == {k: digest(args.source / v) for k, v in filenames.items()}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"n_users": len(selected), "files": manifest["files"]}, indent=2))


if __name__ == "__main__":
    main()
