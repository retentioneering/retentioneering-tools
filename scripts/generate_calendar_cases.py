#!/usr/bin/env python3
"""Reproducible calendar review cases derived from the bundled ecom demo.

Run from the checkout: uv run python scripts/generate_calendar_cases.py
Add --output-dir /tmp/calendar-cases to export reloadable CSV + JSON cases.
The source is synthetic demo data, not production logs. Replay cases preserve
events, sessions, segments, event order and elapsed intervals within each user;
only each user's starting instant is moved to a controlled calendar boundary.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import random
import sys
from dataclasses import asdict, dataclass
from importlib.resources import files
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from retentioneering.datasets import load_ecom  # noqa: E402
from retentioneering.eventstream.eventstream import Eventstream  # noqa: E402


@dataclass
class CalendarCase:
    name: str
    frame: pd.DataFrame
    schema: dict
    provenance: dict


def generate_cases(n_users: int = 8, seed: int = 42) -> list[CalendarCase]:
    """Select complete user paths, then build ten calendar review cases.

    ``seed`` selects users locally, without touching global random state.
    load_ecom's n_users argument is intentionally not used: it only affects
    fallback generation, not selection from the bundled CSV.
    """
    if n_users < 1:
        raise ValueError("n_users must be positive")
    source = load_ecom()
    schema = asdict(source.schema)
    path, ts = source.schema.path_col, source.schema.timestamp_col
    users = sorted(source.df[path].unique())
    if n_users > len(users):
        raise ValueError(f"n_users={n_users} exceeds the {len(users)} available users")
    selected = sorted(random.Random(seed).sample(users, n_users))
    base = source.df.loc[source.df[path].isin(selected)].copy().reset_index(drop=True)
    bundled = files("retentioneering.datasets").joinpath("ecom.csv.gz")
    provenance = {
        "source": "retentioneering.datasets.load_ecom (synthetic demo)",
        "source_sha256": hashlib.sha256(bundled.read_bytes()).hexdigest()
        if bundled.is_file()
        else None,
        "fallback_seed": 42 if not bundled.is_file() else None,
        "selection_seed": seed,
        "users": selected,
    }
    cases = [
        CalendarCase(
            "original-naive",
            base.copy(),
            schema.copy(),
            {
                **provenance,
                "transformation": "none",
            },
        )
    ]
    elapsed = base[ts] - base.groupby(path, observed=True)[ts].transform("min")
    zones = [
        ("fixed-minus-0330", dt.timezone(dt.timedelta(hours=-3, minutes=-30))),
        ("fixed-plus-0545", dt.timezone(dt.timedelta(hours=5, minutes=45))),
        ("misleading-utc-name", dt.timezone(dt.timedelta(hours=5, minutes=45), "UTC")),
        (
            "quoted-custom-name",
            dt.timezone(dt.timedelta(hours=-3, minutes=-30), "Export's custom zone"),
        ),
        ("kathmandu", ZoneInfo("Asia/Kathmandu")),
        ("new-york", ZoneInfo("America/New_York")),
    ]
    anchors = [
        (name, pd.Timestamp("2024-02-28 23:59:59").tz_localize(tz))
        for name, tz in zones
    ] + [
        (
            "dst-spring",
            pd.Timestamp("2024-03-10 06:59:59Z").tz_convert(
                ZoneInfo("America/New_York")
            ),
        ),
        (
            "dst-fall",
            pd.Timestamp("2024-11-03 05:59:59Z").tz_convert(
                ZoneInfo("America/New_York")
            ),
        ),
        ("nanosecond-midnight", pd.Timestamp("2024-02-28 23:59:59.999999999")),
    ]
    for name, anchor in anchors:
        frame = base.copy()
        frame[ts] = anchor + elapsed
        cases.append(
            CalendarCase(
                name,
                frame,
                schema.copy(),
                {
                    **provenance,
                    "transformation": "per-user elapsed-time replay",
                    "anchor": anchor.isoformat(),
                },
            )
        )
    return cases


def expected_calendar(case: CalendarCase) -> pd.DataFrame:
    """Independent Python oracle for all daily states, with no dormant tail."""
    path, ts = case.schema["path_cols"][0], case.schema["timestamp_col"]
    rows = []
    for user, group in case.frame.groupby(path, observed=True, sort=True):
        active = set(group[ts].dt.date)
        day, end, previous = min(active), max(active), None
        while day <= end:
            gap = (day - previous).days if previous is not None else None
            if day in active:
                state = (
                    "new"
                    if gap is None
                    else "current"
                    if gap <= 7
                    else "reactivated"
                    if gap <= 30
                    else "resurrected"
                )
                previous = day
            else:
                state = (
                    "at_risk_wau"
                    if gap <= 7
                    else "at_risk_mau"
                    if gap <= 30
                    else "dormant"
                )
            rows.append({path: user, "day": day, "state": state})
            day += dt.timedelta(days=1)
    return pd.DataFrame(rows)


def validate_case(case: CalendarCase) -> dict:
    """Check day counts and complete state calendars before/after a SQL filter."""
    path, ts = case.schema["path_cols"][0], case.schema["timestamp_col"]
    event = case.schema["event_col"]
    expected_counts = (
        case.frame.assign(_day=case.frame[ts].dt.date)
        .groupby(path, observed=True)["_day"]
        .nunique()
        .to_dict()
    )
    calendar = expected_calendar(case)
    expected_states = {
        (r[path], r["day"]): r["state"] for r in calendar.to_dict("records")
    }
    stream = Eventstream(case.frame.copy(deep=True), case.schema, preprocess=False)
    filtered = stream.filter_events(sql="SELECT * FROM eventstream")
    for label, candidate in [("source", stream), ("sql-filter", filtered)]:
        counts = candidate.get_metrics([{"metric": "active_days"}])[
            "active_days"
        ].to_dict()
        if counts != expected_counts:
            raise AssertionError(
                f"{case.name}/{label}: active_days differs from pandas"
            )
        daily = candidate.to_daily_states(max_dormant_days=0).df
        actual_states = {
            (r[path], r[ts].date()): str(r[event]) for r in daily.to_dict("records")
        }
        if len(daily) != len(calendar) or actual_states != expected_states:
            raise AssertionError(
                f"{case.name}/{label}: daily calendar differs from Python oracle"
            )
    return {
        "case": case.name,
        "events": len(case.frame),
        "users": len(expected_counts),
        "active_user_days": sum(expected_counts.values()),
        "calendar_rows": len(calendar),
        "status": "passed",
    }


def export_case(case: CalendarCase, output_dir: pathlib.Path) -> pathlib.Path:
    """CSV stores instants; the manifest preserves the exact timezone identity."""
    folder = output_dir / case.name
    folder.mkdir(parents=True, exist_ok=False)
    ts = case.schema["timestamp_col"]
    tz = case.frame[ts].dt.tz
    if isinstance(tz, dt.timezone):
        zone = {
            "kind": "fixed",
            "offset_seconds": tz.utcoffset(None).total_seconds(),
            "name": tz.tzname(None),
        }
    elif tz is not None:
        zone = {"kind": "named", "name": getattr(tz, "key", None) or str(tz)}
    else:
        zone = {"kind": "naive"}
    frame = case.frame.copy()
    if tz is not None:
        frame[ts] = frame[ts].dt.tz_convert("UTC")
    frame.to_csv(folder / "events.csv", index=False)
    expected_calendar(case).to_csv(folder / "expected_calendar.csv", index=False)
    manifest = {
        "name": case.name,
        "schema": case.schema,
        "provenance": case.provenance,
        "timezone": zone,
        "timestamp_unit": case.frame[ts].dt.unit,
    }
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return folder


def load_case(folder: pathlib.Path) -> CalendarCase:
    """Reload a generated case without treating a custom name as an IANA zone."""
    manifest = json.loads((folder / "manifest.json").read_text())
    frame = pd.read_csv(folder / "events.csv")
    ts, zone = manifest["schema"]["timestamp_col"], manifest["timezone"]
    frame[ts] = pd.to_datetime(frame[ts], format="ISO8601", utc=zone["kind"] != "naive")
    if zone["kind"] == "fixed":
        tz = dt.timezone(dt.timedelta(seconds=zone["offset_seconds"]), zone["name"])
        frame[ts] = frame[ts].dt.tz_convert(tz)
    elif zone["kind"] == "named":
        frame[ts] = frame[ts].dt.tz_convert(ZoneInfo(zone["name"]))
    frame[ts] = frame[ts].dt.as_unit(manifest["timestamp_unit"])
    return CalendarCase(
        manifest["name"], frame, manifest["schema"], manifest["provenance"]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=pathlib.Path)
    args = parser.parse_args()
    for case in generate_cases(args.users, args.seed):
        result = validate_case(case)
        if args.output_dir is not None:
            folder = export_case(case, args.output_dir)
            validate_case(load_case(folder))
            result["folder"] = str(folder)
        print(json.dumps(result))


if __name__ == "__main__":
    main()
