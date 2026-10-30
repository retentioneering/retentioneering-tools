"""``get_ab_transitions_table`` / ``get_ab_steps_table``: the screens in the shape of
the diff matrices (``tools/ab_tables.py``).

The tables are pivots of the frames, so every non-NaN cell must be a cell of the
frame and every row of the frame must be in the table; the delta pivot must be the
difference of the descriptive tools' group matrices; the export must survive a
round trip with its two header rows; and ``styled=True`` must carry the widgets'
encoding.
"""

from __future__ import annotations

import inspect
import math

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.ab_tables import (
    BLUE,
    RED,
    SIGNIFICANT_BORDER,
    TABLE_VALUES,
    ab_table,
    cell_status,
    delta_background,
)

DIFF = ("exp_group", "treatment", "control")
ARM_DIFF = ("arm", "treatment", "control")


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return datasets.load_ab_test()


@pytest.fixture(scope="module")
def edges(stream) -> pd.DataFrame:
    return stream.get_ab_transitions(DIFF)


@pytest.fixture(scope="module")
def cells(stream) -> pd.DataFrame:
    return stream.get_ab_steps(DIFF)


def _same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or a == b
    return a == b


def _assert_table_is_the_frame(table: pd.DataFrame, frame: pd.DataFrame, keys) -> int:
    """Every row of the frame is a group of cells of the table, and every non-NaN
    cell of the table is a cell of the frame; the number of cells compared."""
    row_key, col_key = keys
    values = list(table.columns.get_level_values("stat").unique())
    seen = set()
    compared = 0
    for record in frame.to_dict("records"):
        r, c = record[row_key], record[col_key]
        seen.add((r, c))
        for stat in values:
            got = table.loc[r, (c, stat)]
            want = record[stat]
            assert _same(got, want) or (pd.isna(got) and pd.isna(want)), (r, c, stat)
            compared += 1
    for r in table.index:
        for c in table.columns.get_level_values(0).unique():
            if (r, c) not in seen:
                assert table.loc[r, c].isna().all(), (r, c)
    return compared


class TestPivot:
    def test__transitions_table_is_the_frame(self, stream, edges) -> None:
        table = stream.get_ab_transitions_table(DIFF)
        assert list(table.columns.names) == ["target", "stat"]
        assert table.index.name == "source"
        assert list(table.columns.get_level_values("stat")[:4]) == list(TABLE_VALUES)
        assert _assert_table_is_the_frame(
            table, edges, ("source", "target")
        ) == 4 * len(edges)
        assert table.index[0] == "path_start"
        assert table.columns.get_level_values("target")[-1] == "path_end"
        assert int(table.xs("delta", axis=1, level="stat").notna().sum().sum()) == int(
            edges["delta"].notna().sum()
        )

    def test__steps_table_is_the_frame(self, stream, cells) -> None:
        table = stream.get_ab_steps_table(
            DIFF, values=["group1_value", "delta", "verdict"]
        )
        assert list(table.columns.names) == ["step", "stat"]
        assert table.index.name == "event" and table.index[-1] == "path_end"
        assert list(table.columns.get_level_values("step").unique()) == list(
            range(1, 11)
        )
        _assert_table_is_the_frame(table, cells, ("event", "step"))

    @pytest.mark.parametrize("weight", ["avg_per_path", "proba_in"])
    def test__other_weights_and_itt(self, stream, weight) -> None:
        assignment = datasets.load_ab_test_assignment()
        options = {"n_bootstrap": 200} if weight == "proba_in" else {}
        frame = stream.get_ab_transitions(
            ARM_DIFF, weight, assignment=assignment, **options
        )
        table = stream.get_ab_transitions_table(
            ARM_DIFF, weight, assignment=assignment, values="rel_delta", **options
        )
        _assert_table_is_the_frame(table, frame, ("source", "target"))
        assert table.attrs["population"] == "assignment"

    def test__axes_are_the_widgets(self, stream, edges) -> None:
        import json

        table = stream.get_ab_transitions_table(DIFF)
        payload = json.loads(stream.ab_transitions(DIFF).result)
        assert list(table.index) == payload["row_keys"]
        assert list(table.columns.get_level_values(0).unique()) == payload["col_keys"]

    def test__untested_rows_keep_delta_and_interval(self, stream, edges) -> None:
        table = stream.get_ab_transitions_table(DIFF)
        untested = edges[edges["p_value_adjusted"].isna()]
        assert len(untested)
        for record in untested.to_dict("records"):
            group = table.loc[record["source"], record["target"]]
            assert np.isfinite(group["delta"]) and np.isnan(group["p_value_adjusted"])

    def test__attrs_are_the_frames(self, stream, edges, cells) -> None:
        table = stream.get_ab_transitions_table(DIFF, values=("delta", "p_value"))
        assert table.attrs == {**edges.attrs, "values": ["delta", "p_value"]}
        steps = stream.get_ab_steps_table(DIFF)
        assert steps.attrs == {**cells.attrs, "values": list(TABLE_VALUES)}

    def test__a_filtered_or_empty_frame(self, edges) -> None:
        top = edges[edges["verdict"].isin(["increase", "decrease"])]
        table = ab_table(top, ["delta"])
        assert int(table.notna().sum().sum()) == len(top)
        empty = ab_table(edges.iloc[:0], ["delta"])
        assert empty.shape == (0, 0)


class TestDeltaIsTheGraphsDifference:
    @pytest.mark.parametrize("weight", ["unique_paths", "avg_per_path", "proba_out"])
    def test__transitions(self, stream, weight) -> None:
        options = {"n_bootstrap": 200} if weight == "proba_out" else {}
        table = stream.get_ab_transitions_table(DIFF, weight, values="delta", **options)
        frame = stream.get_ab_transitions(DIFF, weight, **options)
        delta = table.xs("delta", axis=1, level="stat")
        diff_matrix, g1, g2 = stream.transition_graph_data(weight, diff=DIFF)
        if weight == "unique_paths":
            n1, n2 = frame["group1_paths"].iloc[0], frame["group2_paths"].iloc[0]
            expected = g1 / n1 - g2 / n2
        else:
            expected = diff_matrix
        expected = expected.reindex(index=delta.index, columns=delta.columns)
        mask = delta.notna().to_numpy()
        np.testing.assert_allclose(
            delta.to_numpy(dtype=float)[mask],
            expected.to_numpy(dtype=float)[mask],
            rtol=1e-12,
            atol=1e-15,
        )
        # a NaN is an edge in neither arm's graph, or a ratio without a value
        both = (g1 + g2).reindex(index=delta.index, columns=delta.columns).fillna(0)
        absent = ~mask & (both.to_numpy() == 0)
        assert (~mask == absent).all() or weight != "unique_paths"

    def test__steps(self, stream) -> None:
        table = stream.get_ab_steps_table(DIFF, 8, values="delta")
        delta = table.xs("delta", axis=1, level="stat")
        diff_matrix, _, _ = stream.step_matrix_data(max_steps=8, diff=DIFF)
        expected = diff_matrix.reindex(index=delta.index, columns=delta.columns)
        mask = delta.notna().to_numpy()
        np.testing.assert_allclose(
            delta.to_numpy(dtype=float)[mask],
            expected.to_numpy(dtype=float)[mask],
            rtol=1e-12,
            atol=1e-15,
        )
        # where the table is NaN the step matrix is 0 in both arms
        assert (expected.to_numpy(dtype=float)[~mask] == 0).all()


class TestValues:
    @pytest.mark.parametrize(
        ("values", "match"),
        [
            ("dleta", "dleta"),
            (["delta", "nope"], "nope"),
            ([], "non-empty"),
            (["delta", "delta"], "more than once"),
            (["source"], "key of the table"),
            (42, "non-empty"),
            ({"delta": 1}, "non-empty"),
        ],
    )
    def test__bad_values_raise(self, stream, values, match) -> None:
        with pytest.raises(InvalidParameterError, match=match):
            stream.get_ab_transitions_table(DIFF, values=values)

    def test__step_keys_are_refused(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="key of the table"):
            stream.get_ab_steps_table(DIFF, 3, values=["step"])

    def test__getter_errors_pass_through(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="avg_per_path"):
            stream.get_ab_transitions_table(DIFF, "count")
        with pytest.raises(InvalidParameterError, match="max_steps"):
            stream.get_ab_steps_table(DIFF, 0)

    @pytest.mark.parametrize(
        ("table", "getter"),
        [
            ("get_ab_transitions_table", "get_ab_transitions"),
            ("get_ab_steps_table", "get_ab_steps"),
        ],
    )
    def test__signatures_and_docstrings(self, table, getter) -> None:
        get = inspect.signature(getattr(Eventstream, getter)).parameters
        tab = inspect.signature(getattr(Eventstream, table)).parameters
        assert list(tab) == list(get) + ["values", "styled"]
        assert all(tab[n].default == get[n].default for n in get)
        assert tab["values"].default == TABLE_VALUES
        assert tab["styled"].default is False
        doc = inspect.getdoc(getattr(Eventstream, table))
        params = inspect.getdoc(getattr(Eventstream, getter))
        params = params.split("Parameters\n----------\n")[1].split("\n\nReturns")[0]
        assert params in doc
        assert "values : str or list of str" in doc and "styled : bool" in doc


class TestExport:
    def test__csv_round_trip(self, stream, tmp_path) -> None:
        for table in (
            stream.get_ab_transitions_table(DIFF),
            stream.get_ab_steps_table(DIFF, 5),
        ):
            path = tmp_path / "table.csv"
            table.to_csv(path)
            back = pd.read_csv(path, header=[0, 1], index_col=0)
            assert list(back.index) == list(table.index)
            assert [tuple(map(str, c)) for c in back.columns] == [
                tuple(map(str, c)) for c in table.columns
            ]
            np.testing.assert_allclose(
                back.to_numpy(dtype=float),
                table.to_numpy(dtype=float),
                rtol=1e-12,
                equal_nan=True,
            )

    def test__to_excel(self, stream, tmp_path) -> None:
        pytest.importorskip("jinja2")  # any pandas Styler needs it
        openpyxl = pytest.importorskip("openpyxl")
        path = tmp_path / "edges.xlsx"
        styled = stream.get_ab_transitions_table(DIFF, styled=True)
        styled.to_excel(path)
        back = pd.read_excel(path, header=[0, 1], index_col=0)
        table = styled.data
        np.testing.assert_allclose(
            back.to_numpy(dtype=float), table.to_numpy(dtype=float), equal_nan=True
        )
        # the colours travel: a significant positive delta has a red fill and a
        # black border
        sheet = openpyxl.load_workbook(path).active
        frame = stream.get_ab_transitions(DIFF)
        sig = frame[frame["p_value_adjusted"] < 0.05].sort_values("delta").iloc[-1]
        row = 3 + list(table.index).index(sig["source"])  # two header rows + names row
        col = 2 + list(table.columns).index((sig["target"], "delta"))
        cell = sheet.cell(row=row + 1, column=col)
        assert cell.value == pytest.approx(sig["delta"])
        fill = cell.fill.fgColor.rgb[-6:].lower()
        assert int(fill[0:2], 16) > int(fill[4:6], 16)  # more red than blue
        assert cell.border.top.style is not None


class TestStyled:
    @pytest.fixture(autouse=True)
    def _jinja2(self):
        pytest.importorskip("jinja2")  # any pandas Styler needs it

    def test__styler_draws_the_encoding(self, stream, edges) -> None:
        styled = stream.get_ab_transitions_table(DIFF, styled=True)
        assert isinstance(styled, pd.io.formats.style.Styler)
        table = styled.data
        pd.testing.assert_frame_equal(table, stream.get_ab_transitions_table(DIFF))
        styled._compute()
        css = {
            (table.index[i], table.columns[j]): dict(props)
            for (i, j), props in styled.ctx.items()
        }
        alpha = edges.attrs["alpha"]
        scale = edges["delta"].abs().max()
        counts = {"significant": 0, "not_significant": 0, "untested": 0}
        for record in edges.to_dict("records"):
            status = cell_status(record, alpha)
            counts[status] += 1
            props = css[(record["source"], (record["target"], "delta"))]
            if status == "significant":
                assert props["border-top"] == f"2px solid {SIGNIFICANT_BORDER}"
                assert props["font-weight"] == "bold"
            elif status == "not_significant":
                assert props["border-top"].startswith("1px solid")
                assert "font-weight" not in props
            else:
                assert props["border-top"].startswith("1px dashed")
                assert props["font-style"] == "italic"
            background = delta_background(record["delta"], scale)
            assert props.get("background-color") == background
        assert counts["significant"] == int((edges["p_value_adjusted"] < alpha).sum())
        html = styled.to_html()
        assert "treatment − control" in html

    def test__background_hues(self) -> None:
        assert delta_background(0.5, 0.5) == "#{:02x}{:02x}{:02x}".format(
            *[round(255 + (c - 255) * 0.75) for c in RED]
        )
        assert delta_background(-0.5, 1.0) == "#{:02x}{:02x}{:02x}".format(
            *[round(255 + (c - 255) * 0.375) for c in BLUE]
        )
        assert delta_background(0.0, 1.0) is None
        assert delta_background(float("nan"), 1.0) is None
        assert delta_background(0.2, 0.0) is None

    def test__steps_styled(self, stream, cells) -> None:
        styled = stream.get_ab_steps_table(
            DIFF, styled=True, values=["delta", "p_value"]
        )
        html = styled.to_html()
        assert SIGNIFICANT_BORDER in html
        assert styled.data.attrs["values"] == ["delta", "p_value"]
