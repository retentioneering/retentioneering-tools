"""``get_ab_compression`` and ``compress_events``: arm-blind vocabulary compression
for the A/B screens (``tools/ab_compression.py``, ``data_processors/compress_events.py``).

A toy stream plants one event for every operation (a rare insertion, a rare
substitution, a chain, a thin synonym, and two rare events on which the screens
disagree); the bundled datasets check the properties a plan must have there: no
artificial transitions, no new self-loops, untouched ``keep`` events, an A/A test
that stays clean and planted effects that survive. Every number of the path must be
what the compressed stream's own screen reports, and permuting the arms must not
change the plan.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.ab_compression import plan_mapping

ARM = ("arm", "treatment", "control")
TOY = ("arm", "a", "b")


# ======================================================================= toy stream
def _toy_paths(seed: int = 0) -> list[tuple[str, list[str]]]:
    """``(segment, events)`` per user.

    - ``X`` (10 users) is inserted between ``catalog`` and ``product``, a transition
      over a thousand paths make directly: drop on both screens.
    - ``Y`` (10 users) sits between ``home`` and ``cart``, which never follow each
      other directly: other on both screens.
    - ``Z`` (10 users) sits between ``home`` and ``product``: 100 paths go there
      directly (a frequent edge), but far more go through one event (``catalog``,
      ``search``): drop for transitions, other for steps. Two of its users repeat
      it, so its self-loops keep it out of the synonym merges.
    - ``W`` (10 users) sits between ``product`` and ``home``, which follow each
      other directly in 5 paths (an edge below ``min_support``) and never with one
      event between: other for transitions, drop for steps.
    - ``checkout`` is always followed by ``pay`` and ``pay`` always preceded by it:
      a chain.
    - ``search_alt`` (8 users) has ``search``'s neighbours: a thin synonym.
    - ``r1`` .. ``r4`` (8 users each) open a path and lead to four different
      events: thin, not synonyms of each other, pooled into ``other``.
    """
    rng = np.random.default_rng(seed)
    paths: list[tuple[str, list[str]]] = []
    templates = (
        [["home", "catalog", "product", "cart", "checkout", "pay"]] * 760
        + [["home", "search", "product", "cart"]] * 560
        + [["home", "catalog", "product"]] * 400
        + [["home", "product", "cart"]] * 100
        + [["home", "catalog", "product", "home", "search", "product"]] * 5
    )
    for events in templates:
        paths.append(("simple", list(events)))
    for i in range(10):
        paths.append(("mixed", ["home", "catalog", "X", "product", "cart"]))
        paths.append(("mixed", ["home", "Y", "cart"]))
        zs = ["Z", "Z"] if i < 2 else ["Z"]
        paths.append(("mixed", ["home", *zs, "product", "cart"]))
        paths.append(("mixed", ["home", "catalog", "product", "W", "home"]))
    for _ in range(8):
        paths.append(("mixed", ["home", "search_alt", "product", "cart"]))
    targets = ["cart", "product", "catalog", "search"]
    for i, target in enumerate(targets, start=1):
        for _ in range(8):
            paths.append(("mixed", [f"r{i}", target]))
    order = rng.permutation(len(paths))
    return [paths[i] for i in order]


@pytest.fixture(scope="module")
def toy() -> Eventstream:
    rows = []
    start = pd.Timestamp("2026-01-01")
    for user, (segment, events) in enumerate(_toy_paths()):
        arm = "a" if user % 2 == 0 else "b"
        for k, event in enumerate(events):
            rows.append(
                {
                    "user_id": user,
                    "event": event,
                    "timestamp": start + pd.Timedelta(minutes=user * 100 + k),
                    "arm": arm,
                    "segment": segment,
                }
            )
    return Eventstream(
        pd.DataFrame(rows),
        {"path_cols": ["user_id"], "segment_cols": ["arm", "segment"]},
    )


def _last(stream, screen, diff=TOY, **kw) -> pd.DataFrame:
    """The plan at the last step of the path: every operation applied."""
    path = stream.get_ab_compression(diff, screen, **kw).attrs["path"]
    return stream.get_ab_compression(diff, screen, step=len(path) - 1, **kw)


def _action(plan: pd.DataFrame, event: str) -> str:
    return plan.set_index("event").loc[event, "action"]


# ======================================================================= operations
class TestOperations:
    def test_rare_insertion_is_dropped_on_both_screens(self, toy):
        for screen in ("transitions", "steps"):
            plan = _last(toy, screen)
            row = plan.set_index("event").loc["X"]
            assert row["action"] == "drop", screen
            assert row["label"] is None
            assert row["bridge_fidelity"] == 1.0

    def test_rare_substitution_is_other_on_both_screens(self, toy):
        for screen in ("transitions", "steps"):
            row = _last(toy, screen).set_index("event").loc["Y"]
            assert row["action"] == "other" and row["label"] == "other", screen
            assert row["bridge_fidelity"] == 0.0

    def test_the_screens_disagree_by_design(self, toy):
        transitions = _last(toy, "transitions").set_index("event")
        steps = _last(toy, "steps").set_index("event")
        # Z: home -> product is a frequent edge (drop joins existing transitions),
        # but paths more often go home -> . -> product (a drop would shift steps)
        assert transitions.loc["Z", "action"] == "drop"
        assert steps.loc["Z", "action"] == "other"
        assert "takes the place of another event" in steps.loc["Z", "reason"]
        # W: product -> home exists directly in 5 paths only (would be an edge
        # below min_support) and never with one event between (an insertion)
        assert transitions.loc["W", "action"] == "other"
        assert "artificial transitions" in transitions.loc["W", "reason"]
        assert steps.loc["W", "action"] == "drop"
        assert "realigns the later steps" in steps.loc["W", "reason"]

    def test_chain_merge(self, toy):
        row = _last(toy, "transitions").set_index("event").loc[["checkout", "pay"]]
        assert (row["action"] == "merge").all()
        assert (row["label"] == "checkout+pay").all()
        assert (row["p_next"] == 1.0).all() and (row["p_prev"] == 1.0).all()

    def test_synonym_merge_pools_thin_cells_without_new_self_loops(self, toy):
        plan = _last(toy, "transitions")
        rows = plan.set_index("event").loc[["search", "search_alt"]]
        assert (rows["action"] == "merge").all()
        assert (rows["label"] == "search/search_alt").all()
        assert (rows["js_distance"] <= 0.1).all()
        compact = toy.compress_events(plan)
        counts = compact.transition_graph_data("count")
        label = "search/search_alt"
        assert counts.loc[label, label] == 0 if label in counts.index else True

    def test_thin_substitutions_pool_into_testable_other_cells(self, toy):
        plan = _last(toy, "transitions")
        rows = plan.set_index("event").loc[[f"r{i}" for i in range(1, 5)]]
        assert (rows["action"] == "other").all()
        path = plan.attrs["path"].set_index("step")
        steps = path.index[path["events"].isin(["r1", "r2", "r3", "r4"])]
        first, last = min(steps), max(steps)
        # pooling four thin events makes one of their edges testable
        assert path.loc[last, "testable"] > path.loc[first - 1, "testable"]
        assert path.loc[last, "untestable"] < path.loc[first - 1, "untestable"]

    def test_keep_and_declared_edges_are_untouched(self, toy):
        plan = _last(toy, "transitions", keep=["X", "checkout"])
        rows = plan.set_index("event")
        assert rows.loc["X", "action"] == "keep"
        assert "declared in keep" in rows.loc["X", "reason"]
        assert rows.loc["checkout", "action"] == "keep"
        assert rows.loc["pay", "action"] == "keep"  # its chain partner is kept
        plan = _last(toy, "transitions", edges=[("Y", "product")])
        assert plan.set_index("event").loc["Y", "action"] == "keep"
        assert "declared edges" in plan.set_index("event").loc["Y", "reason"]

    def test_no_artificial_transitions_on_the_transitions_screen(self, toy):
        path = _last(toy, "transitions").attrs["path"]
        assert (path["artificial_transitions"] == 0).all()
        assert (path["artificial_mass"] == 0).all()

    def test_max_events_caps_the_vocabulary(self, toy):
        plan = toy.get_ab_compression(TOY, max_events=6)
        path = plan.attrs["path"]
        assert path.set_index("step").loc[plan.attrs["chosen_step"], "vocabulary"] <= 6
        assert "max_events" in " ".join(plan["reason"])

    def test_target_picks_a_step(self, toy):
        path = toy.get_ab_compression(TOY).attrs["path"]
        most = path[path["testable"] == path["testable"].max()]
        fewest = most[most["untestable"] == most["untestable"].min()]
        best = int(fewest.sort_values(["median_mde", "step"])["step"].iloc[0])
        assert toy.get_ab_compression(TOY).attrs["chosen_step"] == best
        by_mde = toy.get_ab_compression(TOY, target="mde").attrs["chosen_step"]
        assert by_mde == int(path.loc[path["median_mde"].idxmin(), "step"])
        goal = float(path["median_mde"].max()) + 1
        assert toy.get_ab_compression(TOY, target=goal).attrs["chosen_step"] == 0


# ======================================================================= processors
class TestThroughProcessors:
    def test_compress_events_is_the_composition(self, toy):
        plan = _last(toy, "transitions")
        mapping, merged, others = plan_mapping(plan)
        delete = sorted(e for e, label in mapping.items() if label is None)
        rename = {e: label for e, label in mapping.items() if label not in (None, e)}
        expected = toy.edit_events(rename=rename, delete=delete).collapse_events(
            loops=merged
        )
        assert toy.compress_events(plan).equals(expected)
        assert others == ["other"] and "checkout+pay" in merged

    def test_frame_records_and_mapping(self, toy):
        plan = _last(toy, "steps")
        records = plan[["event", "action", "label"]].to_dict("records")
        assert toy.compress_events(records).equals(toy.compress_events(plan))
        merged = {"search": "find", "search_alt": "find", "X": None}
        expected = toy.edit_events(
            rename={"search": "find", "search_alt": "find"}, delete=["X"]
        ).collapse_events(loops=["find"])
        assert toy.compress_events(merged).equals(expected)

    def test_recipe_replays(self, toy):
        plan = _last(toy, "transitions")
        compact = toy.compress_events(plan)
        op = compact.recipe()[-1]
        assert op["type"] == "compress_events"
        assert op["plan"][0].keys() == {"event", "action", "label"}
        rebuilt = Eventstream.from_recipe(
            toy.to_dataframe(),
            compact.recipe(),
            {"path_cols": ["user_id"], "segment_cols": ["arm", "segment"]},
        )
        assert rebuilt.fingerprint == compact.fingerprint

    def test_bad_plans(self, toy):
        with pytest.raises(InvalidParameterError):
            toy.compress_events(pd.DataFrame({"event": ["X"]}))
        with pytest.raises(InvalidParameterError):
            toy.compress_events([{"event": "X", "action": "explode", "label": "x"}])
        with pytest.raises(InvalidParameterError):
            toy.compress_events(42)
        plan = toy.get_ab_compression(TOY, by="segment", by_pre_exposure=True)
        with pytest.raises(InvalidParameterError, match="one segment"):
            toy.compress_events(plan)
        one = plan[plan["segment"] == "mixed"]
        toy.compress_events(one)

    def test_path_col_of_the_plan(self, toy):
        df = toy.to_dataframe()
        df["session_id"] = (
            df["user_id"].astype(str)
            + "_"
            + (df.groupby("user_id").cumcount() // 2).astype(str)
        )
        codes = pd.factorize(df["session_id"])[0]
        df["arm"] = np.where(codes % 2 == 0, "a", "b")
        sessions = Eventstream(
            df.drop(columns=["segment"]),
            {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]},
        )
        plan = sessions.get_ab_compression(TOY, path_col="session_id")
        assert plan.attrs["path_col"] == "session_id"
        with pytest.raises(InvalidParameterError, match="path_col"):
            sessions.compress_events(plan)
        sessions.compress_events(plan, path_col="session_id")


# ======================================================================= reproduction
def _screen(stream, diff, screen, plan, **kw):
    if screen == "transitions":
        return stream.get_ab_transitions(diff, **kw)
    return stream.get_ab_steps(diff, plan.attrs["max_steps"], **kw)


def _check_reproduces(stream, diff, screen, step, **kw):
    plan = stream.get_ab_compression(diff, screen, step=step, **kw)
    row = plan.attrs["path"].set_index("step").loc[step]
    compact = stream.compress_events(plan)
    population = {k: v for k, v in kw.items() if k in ("assignment", "horizon")}
    frame = _screen(compact, diff, screen, plan, **population)
    tested = np.isfinite(frame["p_value_adjusted"].to_numpy(dtype=float))
    assert len(frame) == row["cells"]
    assert int(tested.sum()) == row["testable"]
    assert np.median(frame["mde"].to_numpy()[tested]) == pytest.approx(
        row["median_mde"], rel=1e-12
    )
    events = compact.to_dataframe()[compact.schema.event_col]
    if "assignment" not in kw:
        assert events.nunique() == row["vocabulary"]


class TestReproducesThePath:
    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_toy_every_step(self, toy, screen):
        path = toy.get_ab_compression(TOY, screen).attrs["path"]
        for step in path["step"]:
            _check_reproduces(toy, TOY, screen, int(step))

    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_northwind_with_assignment(self, screen):
        stream = datasets.load_ab_test()
        assignment = datasets.load_ab_test_assignment()
        plan = stream.get_ab_compression(ARM, screen, assignment=assignment)
        last = int(plan.attrs["path"]["step"].max())
        for step in {0, plan.attrs["chosen_step"], last}:
            _check_reproduces(stream, ARM, screen, step, assignment=assignment)

    def test_mappings_of_every_step(self, toy):
        plan = toy.get_ab_compression(TOY)
        mappings = plan.attrs["mappings"]["all"]
        assert len(mappings) == len(plan.attrs["path"])
        assert all(v == k for k, v in mappings[0].items())
        chosen = mappings[plan.attrs["chosen_step"]]
        rows = plan.set_index("event")
        for event, label in chosen.items():
            assert rows.loc[event, "label"] == label


# ======================================================================= arm-blind
class TestArmBlind:
    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_permuting_the_arms_leaves_the_plan(self, screen):
        stream = datasets.load_ab_test()
        assignment = datasets.load_ab_test_assignment()
        kw = dict(min_path_share=0.1)
        plan = stream.get_ab_compression(ARM, screen, assignment=assignment, **kw)
        permuted = assignment.copy()
        permuted["arm"] = np.random.default_rng(7).permutation(
            permuted["arm"].to_numpy()
        )
        other = stream.get_ab_compression(ARM, screen, assignment=permuted, **kw)
        assert plan.equals(other)
        assert plan.attrs["path"].equals(other.attrs["path"])
        swapped = stream.get_ab_compression(
            ("arm", "control", "treatment"), screen, assignment=assignment, **kw
        )
        assert plan.equals(swapped)
        assert plan.attrs["path"].equals(swapped.attrs["path"])

    def test_permuting_the_toy_arms(self, toy):
        df = toy.to_dataframe()
        users = df["user_id"].drop_duplicates()
        flipped = pd.Series(
            np.random.default_rng(1).permutation(np.where(users % 3 == 0, "a", "b")),
            index=users.to_numpy(),
        )
        df["arm"] = df["user_id"].map(flipped)
        other = Eventstream(
            df, {"path_cols": ["user_id"], "segment_cols": ["arm", "segment"]}
        )
        for screen in ("transitions", "steps"):
            mine = toy.get_ab_compression(TOY, screen)
            theirs = other.get_ab_compression(TOY, screen)
            # the arm sizes differ (a third against half), so the MDE does; the
            # operations and their evidence do not depend on the arms
            cols = ["event", "action", "label", "step", "path_share", "bridge_fidelity"]
            assert (
                mine.attrs["path"]["operation"].tolist()
                == theirs.attrs["path"]["operation"].tolist()
            )
            last = len(mine.attrs["path"]) - 1
            a = toy.get_ab_compression(TOY, screen, step=last)[cols]
            b = other.get_ab_compression(TOY, screen, step=last)[cols]
            pd.testing.assert_frame_equal(a, b)


# ======================================================================= bundled data
BUNDLED = list(datasets.list_ab_tests())


def _diff_of(name: str) -> tuple:
    table = datasets.load_ab_test_assignment(name)
    arms = sorted(pd.unique(table["arm"]).tolist(), reverse=True)
    return ("arm", arms[0], arms[1])


def _unit(name: str) -> dict:
    """``path_col`` of the dataset's randomised unit (the assignment table's index)."""
    table = datasets.load_ab_test_assignment(name)
    return {"path_col": table.index.name} if table.index.name == "session_id" else {}


class TestBundled:
    @pytest.mark.parametrize("name", BUNDLED)
    def test_default_plans(self, name):
        stream = datasets.load_ab_test(name)
        table = datasets.load_ab_test_assignment(name)
        diff = _diff_of(name)
        keep = [e for e in ("quick_add",) if e in set(stream.to_dataframe()["event"])]
        for screen in ("transitions", "steps"):
            plan = stream.get_ab_compression(
                diff, screen, assignment=table, keep=keep, **_unit(name)
            )
            path = plan.attrs["path"]
            # no artificial transition at any step of a default plan
            assert (path["artificial_transitions"] == 0).all(), (name, screen)
            for event in keep:
                assert plan.set_index("event").loc[event, "action"] == "keep"
            compact = stream.compress_events(plan, **_unit(name))
            merged = plan.loc[plan["action"] == "merge", "label"].unique()
            if len(merged):
                # at the plan's grain: a merged run is collapsed within a path
                grain = _unit(name).get("path_col")
                counts = compact.transition_graph_data("count", path_col=grain)
                original = stream.transition_graph_data("count", path_col=grain)
                for label in merged:
                    members = plan.loc[plan["label"] == label, "event"]
                    before = sum(
                        original.loc[m, m]
                        for m in members
                        if m in original.index and m in original.columns
                    )
                    after = (
                        counts.loc[label, label]
                        if label in counts.index and label in counts.columns
                        else 0
                    )
                    # no G -> G beyond the members' own self-loops
                    assert after <= before, (name, screen, label)

    def test_northwind_planted_effects_survive(self):
        # Northwind is dense (90 of 92 edges testable): the default plan keeps the
        # stream as it is, so a cap on the vocabulary forces a real compression
        stream = datasets.load_ab_test()
        table = datasets.load_ab_test_assignment()
        assert (
            stream.get_ab_compression(ARM, assignment=table).attrs["chosen_step"] == 0
        )
        before = stream.get_ab_transitions(ARM, assignment=table)
        plan = stream.get_ab_compression(
            ARM, assignment=table, keep=["quick_add", "cart"], max_events=10
        )
        assert (plan["action"] != "keep").sum() >= 4
        compact = stream.compress_events(plan)
        after = compact.get_ab_transitions(ARM, assignment=table)

        def found(frame):
            hits = frame[frame["verdict"].isin(["increase", "decrease"])]
            return set(zip(hits["source"], hits["target"]))

        for edge in [
            ("cart", "cart"),
            ("product_view", "quick_add"),
            ("quick_add", "cart"),
        ]:
            assert edge in found(before) and edge in found(after), edge
        share = lambda f: np.isfinite(f["p_value_adjusted"]).mean()  # noqa: E731
        assert share(after) > share(before)

        steps = stream.get_ab_steps(ARM, 8, assignment=table)
        splan = stream.get_ab_compression(
            ARM,
            "steps",
            max_steps=8,
            assignment=table,
            keep=["quick_add"],
            max_events=10,
        )
        csteps = stream.compress_events(splan).get_ab_steps(ARM, 8, assignment=table)
        cells = lambda f: {  # noqa: E731
            (e, s)
            for e, s, v in zip(f["event"], f["step"], f["verdict"])
            if e == "quick_add" and v == "increase"
        }
        assert cells(steps) and cells(steps) <= cells(csteps)

    def test_testable_cells_grow_on_the_session_steps(self):
        # every session opens session_start -> home: the chain merge moves every
        # later event one step earlier, so the step window holds more of each path
        name = "session_assignment"
        stream = datasets.load_ab_test(name)
        table = datasets.load_ab_test_assignment(name)
        plan = stream.get_ab_compression(
            _diff_of(name), "steps", assignment=table, path_col="session_id"
        )
        path = plan.attrs["path"].set_index("step")
        chosen = plan.attrs["chosen_step"]
        assert chosen >= 1
        assert path.loc[chosen, "testable"] > path.loc[0, "testable"]
        assert "session_start+home" in set(plan["label"])

    def test_steps_and_transitions_plans_differ_on_northwind(self):
        # support_chat (8.5 % of users) is rare at min_path_share=0.1: every run
        # sits between two events that also follow each other directly, so the
        # transitions plan drops it; fewer than 90 % of its runs are insertions
        # (paths without it more often take one other event there), so the steps
        # plan relabels it to keep the later steps in place
        stream = datasets.load_ab_test()
        table = datasets.load_ab_test_assignment()
        kw = dict(assignment=table, min_path_share=0.1)
        t = _last(stream, "transitions", ARM, **kw).set_index("event")
        s = _last(stream, "steps", ARM, **kw).set_index("event")
        assert t.loc["support_chat", "action"] == "drop"
        assert t.loc["support_chat", "bridge_fidelity"] >= 0.9
        assert s.loc["support_chat", "action"] == "other"
        assert s.loc["support_chat", "bridge_fidelity"] < 0.9


def _discoveries(frame: pd.DataFrame) -> int:
    return int(frame["verdict"].isin(["increase", "decrease"]).sum())


class TestCalibration:
    """A capped vocabulary forces a real compression (the default plans of these
    dense streams keep them as they are)."""

    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_aa_stays_clean_after_compression(self, screen):
        stream = datasets.load_ab_test("aa_calibration")
        table = datasets.load_ab_test_assignment("aa_calibration")
        diff = _diff_of("aa_calibration")
        plan = stream.get_ab_compression(diff, screen, assignment=table, max_events=6)
        assert (plan["action"] != "keep").sum() >= 4
        compact = stream.compress_events(plan)
        before = _screen(stream, diff, screen, plan, assignment=table)
        after = _screen(compact, diff, screen, plan, assignment=table)
        assert _discoveries(before) <= 1 and _discoveries(after) <= 1

    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_permuted_northwind_stays_clean(self, screen):
        stream = datasets.load_ab_test()
        table = datasets.load_ab_test_assignment().copy()
        table["arm"] = np.random.default_rng(0).permutation(table["arm"].to_numpy())
        plan = stream.get_ab_compression(ARM, screen, assignment=table, max_events=10)
        assert (plan["action"] != "keep").sum() >= 4
        compact = stream.compress_events(plan)
        before = _screen(stream, ARM, screen, plan, assignment=table)
        after = _screen(compact, ARM, screen, plan, assignment=table)
        assert _discoveries(before) <= 1 and _discoveries(after) <= 1


# ======================================================================= homogeneity
class TestHomogeneity:
    def test_plan_per_segment(self, toy):
        plan = toy.get_ab_compression(TOY, by="segment", by_pre_exposure=True)
        assert plan.columns[0] == "segment"
        assert set(plan["segment"]) == {"mixed", "simple"}
        summary = {h["segment"]: h for h in plan.attrs["homogeneity"]}
        mixed, simple = summary["mixed"], summary["simple"]
        # the heterogeneous segment has the richer vocabulary, the less
        # predictable transitions and more distinct paths, and the thinner screen
        assert mixed["effective_vocabulary"] > simple["effective_vocabulary"]
        assert mixed["transition_entropy"] > simple["transition_entropy"]
        assert mixed["distinct_path_share"] > simple["distinct_path_share"]
        share = lambda h: h["testable_before"] / h["cells_before"]  # noqa: E731
        assert share(mixed) < share(simple)
        for h in (mixed, simple):
            for key in ("steps", "chosen_step", "testable_after", "vocabulary_after"):
                assert key in h
        # each segment's plan lists its own events only
        events = plan.groupby("segment")["event"].apply(set)
        assert "checkout" in events["simple"] and "checkout" not in events["mixed"]
        assert "X" in events["mixed"] and "X" not in events["simple"]
        path = plan.attrs["path"]
        assert set(path["segment"]) == {"mixed", "simple"}
        assert set(plan.attrs["chosen_step"]) == {"mixed", "simple"}

    def test_segment_not_known_pre_exposure_warns(self, toy):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            plan = toy.get_ab_compression(TOY, by="segment")
        assert any("by_pre_exposure" in str(w.message) for w in caught)
        assert plan.attrs["by_pre_exposure"] is False

    def test_declared_pre_exposure(self, toy):
        plan = toy.get_ab_compression(TOY, by="segment", by_pre_exposure=True)
        assert plan.attrs["by_pre_exposure"] is True
        assert plan.attrs["by_pre_exposure_source"] == "declared"


# ======================================================================= validation
class TestValidation:
    def test_arguments_of_the_other_screen(self, toy):
        with pytest.raises(InvalidParameterError, match="max_steps"):
            toy.get_ab_compression(TOY, "transitions", max_steps=5)
        with pytest.raises(InvalidParameterError, match="edge_weight"):
            toy.get_ab_compression(TOY, "steps", edge_weight="unique_paths")
        with pytest.raises(InvalidParameterError, match="edges"):
            toy.get_ab_compression(TOY, "steps", edges=[("home", "catalog")])

    @pytest.mark.parametrize(
        "kw",
        [
            {"screen": "graph"},
            {"keep": ["nope"]},
            {"other": "home"},
            {"other": "a->b"},
            {"by": "nope"},
            {"target": "fast"},
            {"target": -1},
            {"step": -1},
            {"step": 999},
            {"min_path_share": 2},
            {"chain_threshold": 0},
            {"max_events": 0},
            {"by_pre_exposure": True},
            {"edge_weight": "count"},
        ],
    )
    def test_bad_values(self, toy, kw):
        with pytest.raises(InvalidParameterError):
            toy.get_ab_compression(TOY, **kw)

    def test_weights(self, toy):
        for weight in ("avg_per_path", "proba_out", "proba_in", "share_of_total"):
            plan = toy.get_ab_compression(TOY, edge_weight=weight)
            path = plan.attrs["path"]
            assert path["cells"].iloc[0] == len(toy.get_ab_transitions(TOY, weight))
            assert plan.attrs["edge_weight"] == weight
