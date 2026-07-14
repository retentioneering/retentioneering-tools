#!/usr/bin/env python
"""Profile an event log and suggest a Retentioneering Eventstream schema.

Usage:
    python inspect_event_log.py PATH [--sep SEP] [--user-col C] [--event-col C]
                                [--ts-col C] [--sample-rows N] [--out DIR]

Reads CSV/TSV/Parquet, infers user/event/timestamp columns when not given,
runs data-quality checks, writes artifacts/data-profile.json, and prints a
human report plus a ready-to-paste Eventstream(...) snippet.

Only requires pandas. If retentioneering is importable, its version is recorded.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd

USER_HINTS = (
    "user_id",
    "user",
    "client_id",
    "customer_id",
    "uid",
    "session_id",
    "hashed",
    "player",
    "visitor",
)
EVENT_HINTS = ("event_name", "event", "action", "event_type_name", "page", "screen")
TS_HINTS = ("timestamp", "event_time", "time", "datetime", "ts", "date")
SEGMENT_HINTS = (
    "device",
    "platform",
    "country",
    "source",
    "utm",
    "channel",
    "plan",
    "os",
    "browser",
    "campaign",
    "medium",
    "group_name",
    "variant",
)


def sniff_sep(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        head = fh.readline()
    for sep in ("\t", ";", ",", "|"):
        if head.count(sep) >= 2:
            return sep
    return ","


def pick(columns, hints, taken):
    lowered = {c.lower(): c for c in columns}
    for h in hints:
        for lc, orig in lowered.items():
            if h == lc and orig not in taken:
                return orig
    for h in hints:
        for lc, orig in lowered.items():
            if h in lc and orig not in taken:
                return orig
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path")
    ap.add_argument("--sep", default=None)
    ap.add_argument("--user-col", default=None)
    ap.add_argument("--event-col", default=None)
    ap.add_argument("--ts-col", default=None)
    ap.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="read only the first N rows (for very large files)",
    )
    ap.add_argument("--out", default="artifacts")
    a = ap.parse_args()

    if a.path.endswith((".parquet", ".pq")):
        df = pd.read_parquet(a.path)
        sep = None
    else:
        sep = a.sep or sniff_sep(a.path)
        df = pd.read_csv(a.path, sep=sep, nrows=a.sample_rows, low_memory=False)

    cols = list(df.columns)
    taken: set = set()
    user_col = a.user_col or pick(cols, USER_HINTS, taken)
    taken.add(user_col)
    event_col = a.event_col or pick(cols, EVENT_HINTS, taken)
    taken.add(event_col)
    ts_col = a.ts_col or pick(cols, TS_HINTS, taken)
    taken.add(ts_col)
    segment_cols = [
        c
        for c in cols
        if c not in taken
        and any(h in c.lower() for h in SEGMENT_HINTS)
        and df[c].nunique(dropna=True) <= max(50, len(df) // 100)
    ]

    problems: list[str] = []
    if not user_col or not event_col:
        problems.append(
            "could not infer user/event columns — pass --user-col/--event-col"
        )

    profile: dict = {
        "path": os.path.abspath(a.path),
        "sep": sep,
        "rows_read": int(len(df)),
        "sampled": a.sample_rows is not None,
        "columns": cols,
        "inferred": {
            "user_col": user_col,
            "event_col": event_col,
            "ts_col": ts_col,
            "segment_candidates": segment_cols,
        },
    }

    if user_col and event_col:
        nulls_user = int(df[user_col].isna().sum())
        nulls_event = int(df[event_col].isna().sum())
        profile["n_paths"] = int(df[user_col].nunique(dropna=True))
        profile["n_event_types"] = int(df[event_col].nunique(dropna=True))
        profile["nulls"] = {"user": nulls_user, "event": nulls_event}
        if nulls_user:
            problems.append(
                f"{nulls_user} rows with null {user_col} — Eventstream will "
                "reject them; decide drop/repair explicitly"
            )
        top = df[event_col].value_counts()
        profile["top_events"] = {str(k): int(v) for k, v in top.head(25).items()}
        share = float(top.iloc[0]) / max(len(df), 1)
        if share > 0.30:
            problems.append(
                f"event '{top.index[0]}' is {share:.0%} of all rows — likely "
                "noise (heartbeat/dialogue); consider drop_events before path tools"
            )
        plen = df.groupby(user_col, dropna=True).size()
        profile["path_length"] = {
            q: float(plen.quantile(x))
            for q, x in [("p50", 0.5), ("p90", 0.9), ("p99", 0.99)]
        } | {"max": int(plen.max()), "n_len1": int((plen == 1).sum())}
        if plen.max() > 50 * max(plen.median(), 1):
            problems.append(
                f"max path length {plen.max()} vs median {plen.median():.0f} "
                "— check bots/shared accounts"
            )

    ts_ok = False
    if ts_col:
        parsed = pd.to_datetime(df[ts_col], errors="coerce", utc=True, format="mixed")
        bad = int(parsed.isna().sum())
        ts_ok = bad == 0
        profile["timestamp"] = {
            "unparseable": bad,
            "min": str(parsed.min()),
            "max": str(parsed.max()),
        }
        if bad:
            problems.append(f"{bad} unparseable timestamps in {ts_col}")
        if user_col:
            ties = int(df.assign(_p=parsed).duplicated([user_col, "_p"]).sum())
            profile["timestamp"]["within_path_ties"] = ties
            if ties:
                problems.append(
                    f"{ties} same-timestamp ties within paths — ensure a "
                    "stable secondary order (row order is preserved)"
                )
        dup = int(df.duplicated([c for c in (user_col, event_col, ts_col) if c]).sum())
        profile["exact_duplicates"] = dup
        if dup:
            problems.append(
                f"{dup} exact duplicate (user,event,ts) rows — dedupe or justify"
            )
    else:
        problems.append(
            "no timestamp column found — if only an order column exists, "
            "synthesize base_date + order*1s and NEVER report durations (gotcha G2)"
        )

    try:
        import retentioneering

        profile["retentioneering_version"] = retentioneering.__version__
    except Exception:
        profile["retentioneering_version"] = None
        problems.append("retentioneering not importable in this environment")

    profile["problems"] = problems

    os.makedirs(a.out, exist_ok=True)
    out_path = os.path.join(a.out, "data-profile.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(profile, fh, indent=2, ensure_ascii=False, default=str)

    print(
        f"rows={profile['rows_read']:,}  paths={profile.get('n_paths', '?'):,}  "
        f"event_types={profile.get('n_event_types', '?')}"
    )
    if ts_col and "timestamp" in profile:
        print(f"period: {profile['timestamp']['min']} .. {profile['timestamp']['max']}")
    print(
        f"inferred: user={user_col}  event={event_col}  ts={ts_col}  "
        f"segments={segment_cols}"
    )
    for p in problems:
        print(f"  ⚠ {p}")
    print(f"\nprofile written: {out_path}\n")
    print("suggested schema:\n")
    seg = ", ".join(f'"{c}"' for c in segment_cols)
    print(f"""from retentioneering import Eventstream
stream = Eventstream(df, schema={{
    "path_cols": ["{user_col}"],
    "event_cols": ["{event_col}"],
    "timestamp_col": "{ts_col}",
    "segment_cols": [{seg}],
}})
stream.describe()""")
    return 1 if (not user_col or not event_col or not ts_ok) else 0


if __name__ == "__main__":
    sys.exit(main())
