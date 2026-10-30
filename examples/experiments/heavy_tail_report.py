"""Render auditable comparison tables from the frozen JSON/JSON.gz runs."""

import argparse
import gzip
import json
from pathlib import Path


def load(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as source:
        return json.load(source)


def percentage(value):
    if value["rate"] is None:
        return "unavailable"
    lo, hi = value["mc95"]
    return f"{100 * value['rate']:.2f} [{100 * lo:.2f}, {100 * hi:.2f}]"


def render(paths):
    lines = [
        "# Heavy-tail comparison: frozen results",
        "",
        "Read [the protocol and method definitions](HEAVY_TAILS.md).",
        "Brackets are pointwise exact 95% Monte Carlo intervals; rates are percentages.",
        "Availability and joint success are separate from coverage among available intervals.",
    ]
    for path in paths:
        report = load(path)
        config = report["config"]
        lines.extend(
            [
                "",
                f"## {path.name}",
                "",
                f"R={config['trials']}; B={config['resamples']}; seed={config['seed']}; gamma={config['m_power']}.",
                "",
                "| Scenario | Method | Available | Absolute-effect coverage [MC95] | Reject zero [MC95] | Width median / p99 |",
                "|---|---|---:|---|---|---:|",
            ]
        )
        for s in report["results"]:
            for row in s["results"]:
                if row["estimand"] != "absolute_effect":
                    continue
                width = row["width_quantiles"]
                width = f"{width['median']:.3g} / {width['p99']:.3g}" if width else "—"
                lines.append(
                    f"| {s['scenario']['name']} | {row['method']} | {row['availability']['count']}/{row['availability']['n']} | {percentage(row['coverage_available'])} | {percentage(row['rejection_available'])} | {width} |"
                )
        lines.extend(
            [
                "",
                "Rejection is false-positive rate only for `_null` and deterministic zero/one boundary scenarios; under `_effect` it is power.",
                "",
            ]
        )
        if "primary" not in path.name:
            continue
        lines.extend(
            [
                "### Monetary arm and relative-effect coverage",
                "",
                "| Scenario | Method | Estimand | Available | Coverage [MC95] |",
                "|---|---|---|---:|---|",
            ]
        )
        for s in report["results"]:
            if s["scenario"]["kind"] != "money":
                continue
            for row in s["results"]:
                if row["estimand"] == "absolute_effect":
                    continue
                lines.append(
                    f"| {s['scenario']['name']} | {row['method']} | {row['estimand']} | {row['availability']['count']}/{row['availability']['n']} | {percentage(row['coverage_available'])} |"
                )
        lines.extend(
            [
                "",
                "### Paired absolute-effect coverage improvements",
                "",
                "Joint coverage differences include unavailable outcomes in the denominator. MCSE uses paired trial indicators.",
                "",
                "| Scenario | Method | Gain (pp) | Paired MCSE (pp) | Improved / worsened trials |",
                "|---|---|---:|---:|---:|",
            ]
        )
        for s in report["results"]:
            if s["scenario"]["kind"] != "money":
                continue
            for row in s["results"]:
                if (
                    row["estimand"] != "absolute_effect"
                    or row["method"] == "percentile"
                ):
                    continue
                paired = row["paired_joint_coverage_gain"]
                lines.append(
                    f"| {s['scenario']['name']} | {row['method']} | {100 * paired['estimate']:.2f} | {100 * paired['mcse']:.2f} | {paired['improved']} / {paired['worsened']} |"
                )
        lines.extend(
            [
                "",
                "### Separately declared production tests",
                "",
                "These p-values are not derived from bootstrap interval exclusion. Ratio tests are unavailable.",
                "",
                "| Scenario | Test | Available [MC95] | Rejection over all trials [MC95] |",
                "|---|---|---|---|",
            ]
        )
        for s in report["results"]:
            test = s["separate_test"]
            name = test["method"] if test["availability"]["count"] else "unavailable"
            lines.append(
                f"| {s['scenario']['name']} | {name} | {percentage(test['availability'])} | {percentage(test['rejection_all'])} |"
            )
    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(render(args.inputs))
