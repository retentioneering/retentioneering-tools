"""Offline loading with receipt checks. Dataset rights are independent of code rights."""

import hashlib
import json
from pathlib import Path

import pandas as pd


def load_lumeo_tables(data_dir=None):
    root = Path(data_dir) if data_dir else Path(__file__).resolve().parents[1] / "data"
    metadata = json.loads((root / "manifest.json").read_text())
    tables = {}
    for name, info in metadata["files"].items():
        p = root / info["path"]
        if p.resolve().parent != root.resolve():
            raise ValueError(
                "Manifest paths must refer to direct data directory children"
            )
        if hashlib.sha256(p.read_bytes()).hexdigest() != info["sha256"]:
            raise ValueError(f"Checksum mismatch: {name}")
        df = pd.read_csv(p)
        if len(df) != info["rows"] or list(df) != info["columns"]:
            raise ValueError(f"Schema/count mismatch: {name}")
        for col in ("timestamp", "assigned_at", "opened_at", "closed_at"):
            if col in df:
                df[col] = pd.to_datetime(df[col], format="mixed")
        tables[name] = df
    a, e = tables["assignments"], tables["events"]
    if a.user_id.isna().any() or not a.user_id.is_unique:
        raise ValueError("Assignments require one nonnull row per user")
    if not e.user_id.isin(a.user_id).all():
        raise ValueError("Event user missing from assignments")
    if e.groupby("session_id").user_id.nunique().gt(1).any():
        raise ValueError("A session belongs to multiple users")
    for name in ("screens", "clicks"):
        if not tables[name].user_id.isin(a.user_id).all():
            raise ValueError(f"{name} user missing from assignments")
    s, c = tables["screens"], tables["clicks"]
    if not s.screen_instance_id.is_unique or not c.tap_id.is_unique:
        raise ValueError("Duplicate screen/tap key")
    if not c.screen_instance_id.isin(s.screen_instance_id).all():
        raise ValueError("Click has no screen")
    if not s.n_taps.eq(
        c.groupby("screen_instance_id")
        .size()
        .reindex(s.screen_instance_id, fill_value=0)
        .to_numpy()
    ).all():
        raise ValueError("Screen n_taps disagrees with click rows")
    tables["metadata"] = metadata
    return tables
