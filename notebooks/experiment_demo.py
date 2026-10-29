"""Predeclared teaching fixtures, independent of the selected Lumeo dataset.

The fixed seed is never searched for a desired estimate or significance level.
Truth tables audit event generation; estimators receive events and assignments.
"""

from itertools import product

import numpy as np
import pandas as pd

START = pd.Timestamp("2026-01-02")
END = pd.Timestamp("2026-01-16")
OBSERVATION = {
    "start": START.isoformat(),
    "end": END.isoformat(),
    "assignment_time_col": "assigned_at",
    "complete": True,
}
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["variant"],
    "custom_cols": ["order_value_gbp"],
}
CATALOG = {
    "event": [
        "history_view",
        "app_open",
        "product_view",
        "add_to_cart",
        "cart",
        "checkout_start",
        "order_success",
    ]
}
STEPS = ["add_to_cart", "cart", "checkout_start", "order_success"]
PATTERN = "checkout_start->cart->checkout_start"
ORDER = {"metric": "has_event", "metric_args": {"event": "order_success"}}
ORDERS = {"metric": "event_count", "metric_args": {"event": "order_success"}}
REVENUE = {
    "metric": "event_value_sum",
    "metric_args": {"event": "order_success", "value_col": "order_value_gbp"},
}


def make_demo_dataset(n_per_arm=500, seed=20261029, p_control=0.10, p_treatment=0.13):
    """One unselected event realization, with known population probabilities.

    Fixed arm counts model balanced complete randomization. Do not attach a
    Bernoulli SRM diagnostic to this design. Purchase is drawn before activity;
    non-buyers may also be active. Repeat orders and prices are independent of
    the purchase draw, conditional on being a buyer.
    """
    if n_per_arm < 1 or not 0 <= p_control <= 1 or not 0 <= p_treatment <= 1:
        raise ValueError("Positive arm size and probabilities in [0,1] required")
    rng = np.random.default_rng(seed)
    # An independent stream adds intermediate funnel abandonment without
    # changing the purchase/order/price draws or selecting another realization.
    path_rng = np.random.default_rng(np.random.SeedSequence([seed, 17]))
    n = 2 * n_per_arm
    ids = [f"demo_u{i:05d}" for i in range(n)]
    variants = np.array(["control"] * n_per_arm + ["treatment"] * n_per_arm)
    rng.shuffle(variants)
    probabilities = np.where(variants == "control", p_control, p_treatment)
    buyers = rng.random(n) < probabilities
    active = buyers | (rng.random(n) < 0.55)
    assignments = pd.DataFrame(
        {"user_id": ids, "variant": variants, "assigned_at": pd.Timestamp("2026-01-01")}
    )
    rows, truth = [], []
    for i, user in enumerate(ids):
        variant = variants[i]
        rows.append(
            (
                user,
                f"{user}_history",
                "history_view",
                pd.Timestamp("2025-12-31 12:00"),
                np.nan,
                variant,
            )
        )
        sessions = 1 + int(rng.poisson(0.7)) if active[i] else 0
        orders = 1 + int(rng.poisson(1.2)) if buyers[i] else 0
        mean_price = 80.0 if variant == "control" else 88.0
        prices = rng.gamma(4.0, mean_price / 4.0, orders)
        checkout = bool(buyers[i] or (active[i] and rng.random() < 0.45))
        friction_p = 0.30 if variant == "control" else 0.18
        backtrack = bool(checkout and rng.random() < friction_p)
        added = bool(checkout or (active[i] and path_rng.random() < 0.60))
        cart_seen = bool(checkout or (added and path_rng.random() < 0.70))
        base = pd.Timestamp("2026-01-03") + pd.Timedelta(days=int(rng.integers(0, 10)))
        for session in range(sessions):
            values = prices[np.arange(orders) % sessions == session]
            sequence = [
                ("app_open", np.nan),
                ("product_view", np.nan),
                ("product_view", np.nan),
            ]
            if len(values) or (session == 0 and added):
                sequence.append(("add_to_cart", np.nan))
                if len(values) or cart_seen:
                    sequence.append(("cart", np.nan))
                if len(values) or checkout:
                    sequence.append(("checkout_start", np.nan))
                if session == 0 and backtrack:
                    sequence.extend([("cart", np.nan), ("checkout_start", np.nan)])
                sequence.extend(("order_success", float(value)) for value in values)
            for position, (event, value) in enumerate(sequence):
                timestamp = base + pd.Timedelta(hours=2 * session, seconds=position)
                rows.append(
                    (user, f"{user}_s{session}", event, timestamp, value, variant)
                )
        truth.append(
            {
                "user_id": user,
                "variant": variant,
                "converted": int(buyers[i]),
                "active": int(active[i]),
                "n_sessions": sessions,
                "n_orders": orders,
                "revenue": float(prices.sum()),
                "checkout_backtrack": int(backtrack),
            }
        )
    events = pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "session_id",
            "event",
            "timestamp",
            "order_value_gbp",
            "variant",
        ],
    )
    metadata = {
        "name": "independent_ecommerce_demo_v2",
        "seed": seed,
        "n_per_arm": n_per_arm,
        "selection": "one fixed seed; no outcome or significance selection",
        "assignment_design": "balanced_complete_randomization",
        "purchase_probability": {"control": p_control, "treatment": p_treatment},
        "expected_orders_per_buyer": 2.2,
        "mean_order_price": {"control": 80.0, "treatment": 88.0},
        "extra_path_rng": "SeedSequence([seed,17]); non-checkout active users add with p=.60, then view cart with p=.70",
        "population_truth": {
            "conversion_effect": p_treatment - p_control,
            "rpu_effect": p_treatment * 2.2 * 88 - p_control * 2.2 * 80,
            "aov_effect": 8.0,
        },
        "observation": OBSERVATION,
    }
    return {
        "events": events,
        "assignments": assignments,
        "unit_truth": pd.DataFrame(truth),
        "metadata": metadata,
    }


def make_ratio_fixture():
    """Four users: pooled AOV difference -1.3; mean personal AOV +1.5."""
    plan = [
        ("c1", "control", 1, 10.0),
        ("c2", "control", 9, 10.0),
        ("t1", "treatment", 1, 15.0),
        ("t2", "treatment", 9, 8.0),
    ]
    rows, assignments = [], []
    for user, variant, orders, price in plan:
        assignments.append((user, variant, pd.Timestamp("2026-01-01")))
        rows.append(
            (
                user,
                f"{user}_history",
                "history_view",
                pd.Timestamp("2025-12-31 12:00"),
                np.nan,
                variant,
            )
        )
        for order in range(orders):
            timestamp = pd.Timestamp("2026-01-03") + pd.Timedelta(seconds=order)
            rows.append(
                (user, f"{user}_s0", "order_success", timestamp, price, variant)
            )
    return {
        "events": pd.DataFrame(
            rows,
            columns=[
                "user_id",
                "session_id",
                "event",
                "timestamp",
                "order_value_gbp",
                "variant",
            ],
        ),
        "assignments": pd.DataFrame(
            assignments, columns=["user_id", "variant", "assigned_at"]
        ),
    }


def exact_ratio_bootstrap():
    """All 16 equally likely index samples; independent oracle, not CI coverage."""
    control = np.array([[10.0, 1.0], [90.0, 9.0]])
    treatment = np.array([[15.0, 1.0], [72.0, 9.0]])
    rows = []
    for ci in product(range(2), repeat=2):
        for ti in product(range(2), repeat=2):
            c, t = control[list(ci)], treatment[list(ti)]
            rows.append(
                {
                    "control_indices": ci,
                    "treatment_indices": ti,
                    "pooled_effect": t[:, 0].sum() / t[:, 1].sum()
                    - c[:, 0].sum() / c[:, 1].sum(),
                    "personal_effect": (t[:, 0] / t[:, 1]).mean()
                    - (c[:, 0] / c[:, 1]).mean(),
                }
            )
    return pd.DataFrame(rows)


def make_repeated_sessions(repeats=1):
    """Same eight independent people; repeats only duplicate their session paths."""
    if repeats < 1:
        raise ValueError("repeats must be positive")
    rows = []
    for arm, outcomes in [("control", [0, 0, 1, 1]), ("treatment", [0, 1, 1, 1])]:
        for i, purchased in enumerate(outcomes):
            user = f"{arm}_{i}"
            for repeat in range(repeats):
                sequence = ["app_open", "add_to_cart"] + (
                    ["order_success"] if purchased else []
                )
                for j, event in enumerate(sequence):
                    rows.append(
                        (
                            user,
                            f"{user}_s{repeat}",
                            event,
                            pd.Timestamp("2026-01-03")
                            + pd.Timedelta(seconds=10 * repeat + j),
                            10.0 if event == "order_success" else np.nan,
                            arm,
                        )
                    )
    return pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "session_id",
            "event",
            "timestamp",
            "order_value_gbp",
            "variant",
        ],
    )


def manual_ordered_reach(frame, steps, path_col="user_id"):
    """Literal sequence scan, independent of Retentioneering's SQL implementation."""
    result = {}
    for path, group in frame.groupby(path_col, sort=True):
        position = 0
        for event in group.sort_values("timestamp", kind="stable").event:
            if position < len(steps) and event == steps[position]:
                position += 1
        result[path] = [int(position > j) for j in range(len(steps))]
    return pd.DataFrame.from_dict(result, orient="index", columns=steps)
