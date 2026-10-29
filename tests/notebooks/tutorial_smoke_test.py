"""Smoke test of ``notebooks/ab_testing_tutorial.ipynb``.

Executes the notebook top to bottom with nbclient on the cached fixture of the
Northwind Home dataset (``tests/datasets/fixtures/``): a setup cell injected before the
first cell makes ``load_ab_test`` / ``load_ab_test_assignment`` read the fixture instead
of running the generator. A check cell appended after the last cell asserts the claims
the notebook's prose makes about the printed results, so the narrative cannot drift away
from the numbers.
"""

from __future__ import annotations

import pathlib
import time

import pytest

nbformat = pytest.importorskip("nbformat")
nbclient = pytest.importorskip("nbclient")

ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "notebooks" / "ab_testing_tutorial.ipynb"
FIXTURE_DIR = ROOT / "tests" / "datasets" / "fixtures"
BUDGET_S = 60

SETUP = f"""
import os

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

from retentioneering.datasets import ab_test as _ab_test

_fixture = _ab_test._read_fixture({str(FIXTURE_DIR)!r})
_generate = _ab_test._generate


def _generate_from_fixture(n_users, seed):
    if (n_users, seed) == (_ab_test.DEFAULT_N_USERS, _ab_test.DEFAULT_SEED):
        return tuple(frame.copy() for frame in _fixture)
    return _generate(n_users, seed)


_ab_test._generate = _generate_from_fixture
"""

# Every claim of the prose, against the kernel's variables after the last cell.
CHECKS = """
import json


def _verdicts(frame, key="metric"):
    return dict(zip(frame[key], frame["verdict"]))


# step 1: population and SRM; no event precedes its user's assignment
assert before_assignment == 0
assert not srm["mismatch"] and not srm["strata"]["mismatch"].any()
assert srm["silent_paths"] > 0 and srm["multi_valued_paths"] == 0
assert overview["shape"]["n_paths"] < len(assignment)

# step 2: two reads declared up front. Primary (whole window): inconclusive everywhere;
# secondary (horizon="28D"): conversion, orders and revenue increase
assert set(scorecard["verdict"]) == {"inconclusive"}
assert scorecard.set_index("metric").loc["aov", "ci_basis"] == "path_bootstrap"
assert _verdicts(scorecard_28d) == {
    "conversion": "increase",
    "adders": "inconclusive",
    "orders_per_user": "increase",
    "revenue_per_user": "increase",
    "aov": "inconclusive",
}
assert (scorecard_28d["immature_paths"] > 0).all()

# step 3: the widget draws the same scorecard without an error
assert scorecard_widget.error == "" and json.loads(scorecard_widget.result)

# step 4: the funnel counts paths with events, the scorecard the assignment table
assert set(scorecard["population"]) == {"assignment"}
assert funnel["funnel1_total_paths"] < scorecard["group1_paths"].iloc[0]
assert funnel["funnel2_total_paths"] < scorecard["group2_paths"].iloc[0]
# overall steps grey after Holm; two conditional deltas in opposite directions
assert (funnel_table["delta_conversion_rate_p_value_adjusted"] > 0.05).all()
assert funnel_table.loc["checkout_start", "delta_step_conversion_rate_ci_low"] > 0
assert funnel_table.loc["payment", "delta_step_conversion_rate_ci_high"] < 0
assert float(naive.set_index("step").loc["checkout_start -> payment", "naive_p_value"]) < 1e-3

# step 5: weekly drift, since-assignment bins (the 42D sensitivity check), the contrast
assert weekly["delta"].iloc[0] > 0 > weekly["delta"].iloc[-1]
assert _verdicts(since, "period")["0-13"] == "increase"
assert _verdicts(since, "period")["28-41"] == "decrease"
# the readout quotes the late bin: a decrease of 1.5 pp at adjusted p 0.023
_late = since.set_index("period").loc["28-41"]
assert round(_late["delta"] * 100, 1) == -1.5 and round(_late["p_value_adjusted"], 3) == 0.023
assert since["group1_paths"].nunique() == 1
assert low > 0 and low < point < high

# step 6: the window before the first product_view is short ("about three events per user")
# and balanced between the arms; blind clusters, both signs after Holm
assert set(window_balance["verdict"]) == {"inconclusive"}
_window_length = window_balance.set_index("metric").loc["window_length"]
assert round(_window_length["group1_value"]) == 3 and round(_window_length["group2_value"]) == 3
assert labels.attrs.get("pre_exposure") is True  # the recipe shows the truncation
# markers: search_filter (scanner), collections_view (curator), plain catalog browsing (other)
assert set(marker) == {"search_filter", "collections_view", "catalog"}
assert sorted(cluster_names.values()) == ["curator", "other", "scanner"]
assert _verdicts(strata, "stratum") == {
    "<MISSING>": "insufficient_data",
    "other": "inconclusive",
    "curator": "decrease",
    "scanner": "increase",
}
# "other", the largest group, shows no detectable change: about -2.9 to +1.7 pp
_other = strata.set_index("stratum").loc["other"]
assert round(_other["delta_ci_low"] * 100, 1) == -2.9 and round(_other["delta_ci_high"] * 100, 1) == 1.7
assert _other["group1_paths"] == strata["group1_paths"].max()
assert strata["balance_p_value"].iloc[0] > 0.05
assert strata["heterogeneity_p_value"].iloc[0] < 0.05
assert strata.loc[strata["stratum"] != "<MISSING>", "strata_pre_exposure"].all()
assert (
    strata.loc[strata["stratum"] != "<MISSING>", "strata_pre_exposure_source"] == "declared"
).all()
assert not strata["exploratory"].any()

# step 7: the funnels' last step repeats the strata rows
for _name, _row in cluster_funnels.items():
    _stratum = strata.set_index("stratum").loc[_name]
    assert abs(_row["order_delta"] - _stratum["delta"]) < 1e-12

# step 8: the loop exists in treatment only
assert graph_treatment.loc["cart", "cart"] > 0 and graph_control.loc["cart", "cart"] == 0

# step 9: descriptive loop -> order (conditional), ITT loop share under the pooled support rule
assert loop_to_order.iloc[0]["group2_paths_with_start"] == 0
assert loop_to_order["conditional"].all()
_loop = loop_itt.set_index("stratum")
assert round(_loop.loc["curator", "group1_value"] * 100) == 21
assert _loop.loc["curator", "delta_ci_low"] > 0.1
assert (_loop["group2_value"] == 0).all()
assert (_loop.drop(index="curator")["group1_value"] == 0).all()
assert _loop.loc["curator", "support_warning"] == "one_arm_only"
assert _loop.loc["curator", "verdict"] == "increase"
assert set(_loop.drop(index="curator")["verdict"]) == {"insufficient_data"}
assert set(_loop.drop(index="curator")["support_warning"]) == {"no_data"}

# step 10: the window before the first add is balanced; the sign flips with its length, and
# not with the length of the stricter window; post-exposure split flagged, balance p silent
assert pre_add_balance["verdict"].iloc[0] == "inconclusive"
assert by_strict_length["heterogeneity_p_value"].iloc[0] > 0.05
assert set(by_strict_length.loc[by_strict_length["stratum"] != "<MISSING>", "verdict"]) == {
    "inconclusive"
}
assert by_length["heterogeneity_p_value"].iloc[0] < 0.05
assert _verdicts(by_length, "stratum")["short"] == "decrease"
assert _verdicts(by_length, "stratum")["long"] == "increase"
assert (
    by_length.loc[by_length["stratum"] != "<MISSING>", "strata_pre_exposure_source"]
    == "declared"
).all()
assert not post_length["strata_pre_exposure"].any() and post_length["exploratory"].all()
assert (post_length["strata_pre_exposure_source"] == "not_pre_exposure").all()
assert post_length["balance_p_value"].iloc[0] > 0.05
# ... and it tells a different story from the pre-exposure split
assert _verdicts(post_length, "stratum") != _verdicts(by_length, "stratum")
assert _verdicts(post_length, "stratum")["short"] != "decrease"

# the length split and the clusters overlap only partly: every cluster has short and long users
_overlap = pd.crosstab(
    stream.get_path_segments("behaviour"),
    pre_length.reindex(stream.get_path_segments("behaviour").index),
    normalize="index",
)
assert ((_overlap > 0.1) & (_overlap < 0.9)).all().all()

# step 11: planted truth. Every planted scanner and curator lands in the cluster of that name;
# the scanner and curator clusters also take in some planted "other" users
assert adjusted_rand_score(truth, recovered.reindex(truth.index)) >= 0.5
_truth_table = pd.crosstab(recovered.reindex(truth.index), truth)
for _type in ("scanner", "curator"):
    assert _truth_table[_type].drop(index=_type).sum() == 0
    assert _truth_table.loc[_type, "other"] > 0
print("tutorial checks passed")
"""


def test__tutorial_executes_and_its_claims_hold() -> None:
    nb = nbformat.read(NOTEBOOK, as_version=4)
    assert all(
        not cell.get("outputs") for cell in nb.cells if cell.cell_type == "code"
    ), "commit the notebook with outputs stripped"
    nb.cells.insert(0, nbformat.v4.new_code_cell(SETUP))
    nb.cells.append(nbformat.v4.new_code_cell(CHECKS))

    client = nbclient.NotebookClient(
        nb,
        timeout=BUDGET_S,
        kernel_name="python3",
        resources={"metadata": {"path": str(NOTEBOOK.parent)}},
    )
    started = time.perf_counter()
    client.execute()
    elapsed = time.perf_counter() - started

    last = nb.cells[-1]["outputs"]
    assert any("tutorial checks passed" in out.get("text", "") for out in last)
    assert elapsed < BUDGET_S
