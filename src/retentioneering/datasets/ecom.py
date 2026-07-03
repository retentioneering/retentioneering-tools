"""
Synthetic e-commerce clickstream dataset for retentioneering demos.

Six months of user sessions on a consumer-electronics store. Five embedded
stories make the data useful for teaching Retentioneering's analysis tools:

  1. VIRAL ARTICLE (Feb 29 – Mar 3, days 59-62): a tech blog post drives a
     spike of organic traffic. New visitors browse heavily but rarely buy.

  2. CHECKOUT BUG (Apr 9 – May 9, days 99-130): a deploy breaks the "back"
     button on the shipping page for mobile users, sending them back to
     add_to_cart instead of forward to payment. Creates a visible
     shipping_details → add_to_cart → cart → shipping_details loop.

  3. PAYMENT GATEWAY INCIDENT (May 19 – Jun 7, days 139-159): a misconfigured
     payment provider shows a spurious validation error to ~50 % of users.
     Conversion at payment_details collapses; support_chat spikes.

  4. NEW vs LOYAL USERS: users who registered in the last 30 days (new) browse
     far more and convert far less than loyal users (>90 days). Visible on
     diff transition graph and step-sankey.

  5. COHORT DRIFT: January-cohort users (now loyal) take direct purchase paths;
     April-cohort users still in exploration mode.

Usage
-----
>>> from retentioneering.datasets.ecom import load_ecom
>>> import retentioneering as hs
>>> df = load_ecom()
>>> stream = hs.Eventstream(df, {
...     "path_cols": ["user_id", "session_id"],
...     "segment_cols": [
...         "platform", "acquisition_channel", "user_cohort",
...         "user_age_segment", "is_anomaly_period",
...         "had_checkout_bug", "had_payment_issue",
...     ],
... })
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd

_DATA_FILE = pathlib.Path(__file__).parent / "ecom.csv.gz"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_ecom(n_users: int = 600, seed: int = 42) -> pd.DataFrame:
    """Load the synthetic e-commerce dataset.

    Reads ``ecom.csv.gz`` bundled with the package.  Falls back to
    on-the-fly generation via ``_generate_ecom()`` if the file is missing.

    Returns
    -------
    pd.DataFrame with columns:
        user_id, session_id, event, timestamp,
        platform, acquisition_channel, user_cohort, user_lifecycle

    Notes
    -----
    The dataset also contains two diagnostic events — ``checkout_bug``
    (injected during the mobile back-button bug period) and
    ``payment_error`` (injected during the payment gateway incident).
    Use ``Eventstream.add_segment`` to derive ``had_checkout_bug`` and
    ``had_payment_issue`` segments from these events in your notebook.
    """
    if _DATA_FILE.exists():
        return pd.read_csv(_DATA_FILE, parse_dates=["timestamp"])
    return _generate_ecom(n_users=n_users, seed=seed)


def _generate_ecom(n_users: int = 600, seed: int = 42) -> pd.DataFrame:
    """Regenerate the dataset programmatically (used to produce ecom.csv.gz)."""
    rng = np.random.default_rng(seed)

    START = pd.Timestamp("2024-01-01")
    N_DAYS = 181  # Jan 1 – Jun 30

    # ── User profiles ────────────────────────────────────────────────────────
    reg_day = rng.integers(0, 120, size=n_users)        # register Jan–Apr
    channels = rng.choice(
        ["organic", "paid_search", "email", "social", "direct"],
        size=n_users, p=[0.37, 0.28, 0.15, 0.15, 0.05],
    )
    platforms = rng.choice(
        ["desktop", "mobile", "tablet"],
        size=n_users, p=[0.52, 0.38, 0.10],
    )
    user_ids = [f"user_{i:04d}" for i in range(n_users)]

    # ── Special periods (day indices, 0-based) ───────────────────────────────
    ANOM_START, ANOM_END       = 59,  62   # viral article
    BUG_START,  BUG_END        = 99,  130  # checkout bug
    PAY_START,  PAY_END        = 139, 159  # payment incident

    records: list[dict] = []
    session_counter = 0

    for i, uid in enumerate(user_ids):
        user_reg_day   = int(reg_day[i])
        channel        = channels[i]
        platform_base  = platforms[i]  # dominant platform for this user

        # Number of sessions: 3-9, spread after registration
        available_days = list(range(user_reg_day, N_DAYS))
        if len(available_days) < 2:
            continue
        n_sess = int(rng.integers(3, 10))
        n_sess = min(n_sess, len(available_days))
        sess_days = sorted(rng.choice(available_days, size=n_sess, replace=False))

        for sess_day in sess_days:
            session_counter += 1
            sid = f"sess_{session_counter:06d}"

            # Allow slight platform variance per session (10% chance of different device)
            if rng.random() < 0.10:
                plat = rng.choice(["desktop", "mobile", "tablet"], p=[0.52, 0.38, 0.10])
            else:
                plat = platform_base

            # Special period flags
            is_anom  = ANOM_START <= sess_day <= ANOM_END
            is_bug   = BUG_START  <= sess_day <= BUG_END
            is_pay   = PAY_START  <= sess_day <= PAY_END

            # User lifecycle stage at this session
            age_days = sess_day - user_reg_day
            if age_days < 30:
                lifecycle = "new"
            elif age_days < 90:
                lifecycle = "returning"
            else:
                lifecycle = "loyal"

            cohort = (START + pd.Timedelta(days=user_reg_day)).strftime("%Y-%m")

            # Event sequence
            events = _generate_session(
                rng, lifecycle, plat, channel, is_anom, is_bug, is_pay
            )

            # Timestamps: random hour of day, ~30-180 s between events
            base_ts = START + pd.Timedelta(days=int(sess_day),
                                           hours=int(rng.integers(8, 22)),
                                           minutes=int(rng.integers(0, 59)))
            for e_idx, ev in enumerate(events):
                gap = int(rng.integers(15, 180))
                ts  = base_ts + pd.Timedelta(seconds=gap * (e_idx + 1))
                records.append({
                    "user_id":             uid,
                    "session_id":          sid,
                    "event":               ev,
                    "timestamp":           ts,
                    "platform":            plat,
                    "acquisition_channel": channel,
                    "user_cohort":         cohort,
                    "user_lifecycle":      lifecycle,
                })

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["session_id", "timestamp"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Session generator (state machine)
# ---------------------------------------------------------------------------

def _add_to_cart_prob(age_seg: str, channel: str, platform: str,
                      is_anom: bool) -> float:
    """Base probability of proceeding to add_to_cart from product_view."""
    base = {"loyal": 0.28, "returning": 0.18, "new": 0.10}.get(age_seg, 0.15)
    if channel == "paid_search":  base += 0.08
    if channel == "social":       base -= 0.03
    if platform == "mobile":      base -= 0.04
    if is_anom:                   base *= 0.30   # anomaly visitors rarely buy
    return max(0.03, min(base, 0.45))


def _generate_session(
    rng: np.random.Generator,
    age_seg: str,
    platform: str,
    channel: str,
    is_anom: bool,
    is_bug:  bool,
    is_pay:  bool,
) -> list[str]:
    """Return a list of event strings for one session."""

    events: list[str] = []
    p_atc = _add_to_cart_prob(age_seg, channel, platform, is_anom)

    # ── Entry ────────────────────────────────────────────────────────────────
    if age_seg == "loyal" and rng.random() < 0.25:
        # Loyal users often land directly on product page or cart
        state = rng.choice(["product_view", "add_to_cart"], p=[0.7, 0.3])
    elif channel == "email" and rng.random() < 0.40:
        state = "product_view"  # email campaigns land on product pages
    else:
        state = rng.choice(["home", "catalog", "search"], p=[0.50, 0.30, 0.20])

    events.append(state)
    max_browse_steps = int(rng.integers(4, 22))

    # ── Browse loop ──────────────────────────────────────────────────────────
    for _ in range(max_browse_steps):
        if state in ("purchase", "_exit"):
            break

        nxt = _next_event(rng, state, age_seg, platform, channel,
                          is_anom, is_bug, is_pay, p_atc, events)
        if nxt == "_exit":
            break
        events.append(nxt)
        state = nxt

        # Inject noise events occasionally
        if rng.random() < 0.06:
            noise = rng.choice(["account_page", "promo_page", "wishlist_add",
                                 "error_page", "support_chat"],
                                p=[0.30, 0.25, 0.20, 0.15, 0.10])
            events.append(noise)

    return events


def _next_event(
    rng, state, age_seg, platform, channel,
    is_anom, is_bug, is_pay, p_atc, events_so_far,
) -> str:
    """Transition function: given current state return next event or '_exit'."""

    r = rng.random()

    # ── Noisy back/refresh chance from product page ──────────────────────────
    if state == "product_view" and is_anom and r < 0.10:
        return rng.choice(["error_page", "support_chat"])

    # ── Main transitions ─────────────────────────────────────────────────────
    if state == "home":
        return rng.choice(
            ["catalog", "search", "promo_page", "account_page", "_exit"],
            p=[0.42, 0.32, 0.14, 0.08, 0.04],
        )

    if state == "catalog":
        self_loop_p = 0.18 if is_anom else 0.10
        left = 1 - self_loop_p
        opts  = ["product_view", "search", "filter_results", "catalog", "home", "_exit"]
        probs = [0.42*left, 0.15*left, 0.20*left, self_loop_p, 0.15*left, 0.08*left]
        return rng.choice(opts, p=_norm(probs))

    if state == "search":
        self_loop_p = 0.28 if is_anom else 0.18
        left = 1 - self_loop_p
        opts  = ["product_view", "search", "filter_results", "catalog", "_exit"]
        probs = [0.38*left, self_loop_p, 0.30*left, 0.22*left, 0.10*left]
        return rng.choice(opts, p=_norm(probs))

    if state == "filter_results":
        self_loop_p = 0.22
        left = 1 - self_loop_p
        opts  = ["product_view", "filter_results", "catalog", "_exit"]
        probs = [0.55*left, self_loop_p, 0.35*left, 0.10*left]
        return rng.choice(opts, p=_norm(probs))

    if state == "product_view":
        # Self-loop: viewing multiple product images / social users linger more
        sl = 0.22 if channel == "social" else 0.14
        left = 1 - sl
        p_exit = 0.12 if age_seg == "new" else 0.06
        opts  = ["product_view", "add_to_cart", "catalog", "search",
                 "wishlist_add", "compare", "review_page", "_exit"]
        probs = [sl, p_atc*left, 0.28*left, 0.15*left,
                 0.08*left, 0.05*left, 0.07*left, p_exit*left]
        return rng.choice(opts, p=_norm(probs))

    if state == "wishlist_add":
        return rng.choice(["product_view", "catalog", "add_to_cart", "_exit"],
                          p=[0.45, 0.30, 0.15, 0.10])

    if state == "compare":
        return rng.choice(["product_view", "add_to_cart", "catalog", "_exit"],
                          p=[0.50, 0.25, 0.20, 0.05])

    if state == "review_page":
        return rng.choice(["product_view", "catalog", "add_to_cart", "_exit"],
                          p=[0.55, 0.25, 0.12, 0.08])

    if state == "promo_page":
        return rng.choice(["product_view", "catalog", "home", "_exit"],
                          p=[0.50, 0.30, 0.15, 0.05])

    if state == "account_page":
        return rng.choice(["home", "catalog", "_exit"], p=[0.45, 0.35, 0.20])

    # ── Funnel ───────────────────────────────────────────────────────────────
    if state == "add_to_cart":
        return rng.choice(["cart", "catalog", "_exit"], p=[0.86, 0.10, 0.04])

    if state == "cart":
        abandon_p = 0.22 if platform == "mobile" else 0.14
        return rng.choice(["shipping_details", "catalog", "home", "_exit"],
                          p=_norm([1-abandon_p-0.05, abandon_p*0.55, abandon_p*0.30, 0.05]))

    if state == "shipping_details":
        # CHECKOUT BUG: mobile during bug period → checkout_bug (marker) → add_to_cart
        if is_bug and platform == "mobile" and rng.random() < 0.55:
            return "checkout_bug"
        fwd_p = 0.68 if platform == "mobile" else 0.82
        return rng.choice(["payment_details", "cart", "home", "_exit"],
                          p=_norm([fwd_p, (1-fwd_p)*0.55, (1-fwd_p)*0.35, 0.02]))

    if state == "payment_details":
        # PAYMENT ISSUE: ~70 % of users hit the fake error (payment_error marker event)
        if is_pay and rng.random() < 0.70:
            return "payment_error"
        return rng.choice(["purchase", "shipping_details", "_exit"],
                          p=[0.88, 0.08, 0.04])

    if state == "support_chat":
        if is_pay and rng.random() < 0.40:
            return "payment_details"   # retry — but only 40 % succeed second time
        return rng.choice(["home", "catalog", "_exit"], p=[0.40, 0.30, 0.30])

    if state == "purchase":
        return rng.choice(["review_page", "account_page", "home", "_exit"],
                          p=[0.30, 0.20, 0.45, 0.05])

    if state == "error_page":
        return rng.choice(["home", "catalog", "support_chat", "_exit"],
                          p=[0.35, 0.30, 0.20, 0.15])

    if state == "checkout_bug":
        # Broken back-button: always sends user back to add_to_cart
        return "add_to_cart"

    if state == "payment_error":
        # After the fake gateway error, user seeks support
        return "support_chat"

    return "_exit"


def _norm(probs: list[float]) -> list[float]:
    """Normalise a list of probabilities to sum to 1."""
    s = sum(probs)
    return [p / s for p in probs]
