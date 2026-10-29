"""Matplotlib views of saved results; no recalculation or new random draws."""

import matplotlib.pyplot as plt
import numpy as np

CONTROL, TREATMENT, EFFECT = "#42657d", "#bc6532", "#273a42"


def _number(value):
    """Normalize a JSON-null/pandas scalar without treating missing values as zero."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return np.nan
    return value if np.isfinite(value) else np.nan


def _mark(ax, x, lo, hi, y, color):
    x, lo, hi = (_number(value) for value in (x, lo, hi))
    if np.isfinite(lo) and np.isfinite(hi):
        ax.hlines(y, lo, hi, colors=color, linewidth=2.3)
        ax.plot([lo, hi], [y, y], "|", color=color, markersize=9)
    if np.isfinite(x):
        ax.plot(x, y, "o", color=color, markersize=6)


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x", alpha=0.16)
    ax.set_axisbelow(True)


def _interval_label(result, table=None, prefix="absolute_effect"):
    table = result.summary if table is None else table
    if f"{prefix}_ci_method" not in table:
        return "Point estimates"
    labels = []
    for _, row in table.iterrows():
        method = row.get(f"{prefix}_ci_method")
        level = row.get(f"{prefix}_ci_confidence_level")
        if (
            method is not None
            and isinstance(level, (int, float))
            and np.isfinite(level)
        ):
            name = {
                "newcombe": "Newcombe",
                "wilson": "Wilson",
                "bootstrap_percentile": "percentile bootstrap",
            }.get(method, method)
            label = f"{level:.0%} {name}"
            if label not in labels:
                labels.append(label)
    return (
        " / ".join(labels) + " · pointwise intervals"
        if labels
        else "Point estimates; intervals not requested"
    )


def plot_effects(result, metrics, labels, units, scales=None):
    """One panel per measurement unit; only absolute effects are plotted."""
    scales = scales or [1] * len(metrics)
    if not metrics or not len(metrics) == len(labels) == len(units) == len(scales):
        raise ValueError("Provide one label, unit and scale per nonempty metric list")
    table = result.summary.set_index("metric")
    fig, axes = plt.subplots(
        len(metrics),
        1,
        figsize=(10, max(2.8, 2.05 * len(metrics))),
        squeeze=False,
        layout="constrained",
    )
    for ax, name, label, unit, scale in zip(axes[:, 0], metrics, labels, units, scales):
        r = table.loc[name]
        _style(ax)
        ax.axvline(0, color="#89949b", linestyle="--", linewidth=1)
        _mark(
            ax,
            _number(r.absolute_effect) * scale,
            _number(r.absolute_effect_ci_low) * scale,
            _number(r.absolute_effect_ci_high) * scale,
            0,
            EFFECT,
        )
        ax.set_yticks([])
        ax.set_ylim(-0.65, 0.65)
        ax.set_xlabel(f"Treatment − control · {unit}")
        point = _number(r.absolute_effect) * scale
        estimate = f"{point:+.3g}" if np.isfinite(point) else "unavailable"
        ax.set_title(
            f"{label}  |  estimate {estimate}",
            loc="left",
            fontsize=12,
        )
        if r.absolute_effect_status != "ok":
            ax.text(
                0.98,
                0.8,
                f"CI unavailable: {r.absolute_effect_status}",
                transform=ax.transAxes,
                ha="right",
                fontsize=9,
            )
    fig.suptitle(
        f"Assigned-user outcomes · {_interval_label(result, table.loc[metrics])}",
        fontsize=15,
    )
    return fig


def plot_funnel(result, family="funnel_cumulative", title=None):
    table = result.summary.loc[result.summary.family.eq(family)].sort_values(
        "step_index"
    )
    if table.empty:
        raise ValueError(f"No rows for funnel family: {family}")
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), layout="constrained", sharey=True)
    for ax in axes:
        _style(ax)
    for j, (_, r) in enumerate(table.iterrows()):
        for label, offset, color in (
            ("control", -0.12, CONTROL),
            ("treatment", 0.12, TREATMENT),
        ):
            _mark(
                axes[0],
                _number(r[label]) * 100,
                _number(r[f"{label}_ci_low"]) * 100,
                _number(r[f"{label}_ci_high"]) * 100,
                j + offset,
                color,
            )
            if r[f"{label}_status"] != "ok":
                axes[0].text(
                    0.98,
                    j + offset,
                    f"{label.title()} CI: {r[f'{label}_status']}",
                    transform=axes[0].get_yaxis_transform(),
                    ha="right",
                    va="center",
                    fontsize=7,
                    color=color,
                )
        _mark(
            axes[1],
            _number(r.absolute_effect) * 100,
            _number(r.absolute_effect_ci_low) * 100,
            _number(r.absolute_effect_ci_high) * 100,
            j,
            EFFECT,
        )
        if r.absolute_effect_status != "ok":
            axes[1].text(
                0.98,
                j + 0.15,
                "CI: " + r.absolute_effect_status,
                transform=axes[1].get_yaxis_transform(),
                ha="right",
                va="top",
                fontsize=8,
            )
    axes[0].set_yticks(
        range(len(table)),
        [f"{int(r.step_index)}. {r.step}" for _, r in table.iterrows()],
    )
    # Missing points still occupy a step row; autoscaling only sees finite marks.
    axes[0].set_ylim(len(table) - 0.5, -0.5)
    axes[0].set_xlabel("Control / treatment · %")
    axes[1].set_xlabel("Treatment − control · percentage points")
    axes[1].axvline(0, color="#89949b", linestyle="--", linewidth=1)
    axes[0].plot([], [], "o", color=CONTROL, label="Control")
    axes[0].plot([], [], "o", color=TREATMENT, label="Treatment")
    axes[0].legend(loc="best", frameon=False)
    if family == "funnel_conditional":
        axes[0].set_xlim(-2, 105)
    fig.suptitle(title or family.replace("_", " ").title(), fontsize=15)
    fig.supxlabel(
        f"Arms: {_interval_label(result, table, 'control')} · Difference: {_interval_label(result, table)}\n"
        f"Independent unit: {result.metadata['unit_col']} · unavailable intervals retain a diagnostic",
        fontsize=10,
    )
    return fig
