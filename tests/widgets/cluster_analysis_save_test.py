"""Save and copied code must replay the displayed clustering pipeline."""

import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.cluster_analysis import ClusterAnalysisWidget
from tests.data_processors.add_clusters_test import get_scale_conflict_df


class TestClusterAnalysisWidgetResultSnapshot:
    @staticmethod
    def _widget():
        frame = get_scale_conflict_df().rename(columns={"user_id": "visitor"})
        stream = Eventstream(frame, schema={"path_cols": ["visitor"]})
        widget = ClusterAnalysisWidget(
            stream,
            features=[{"metric": "length"}],
            method_args={"n_clusters": 2},
        )
        assert widget.error == ""
        return stream, widget

    @pytest.mark.parametrize(
        "updates",
        [
            {"features": json.dumps([{"metric": "duration"}])},
            {"features": "[]"},
            {"path_col": "missing_path"},
            {
                "method": "hdbscan",
                "scaler": "std",
                "nmf_enabled": True,
                "nmf_components": "1",
                "n_clusters": "3",
            },
        ],
        ids=["features", "empty-features", "path", "pipeline"],
    )
    def test__save_uses_the_shown_result_before_pending_edits(self, updates) -> None:
        stream, widget = self._widget()
        shown = widget._cluster_labels.copy()
        snapshot = json.loads(widget.save_params)
        for key, value in updates.items():
            setattr(widget, key, value)
        rename = {label: f"named_{label}" for label in shown.unique()}
        widget.save_segment_name = "saved"
        widget.save_rename = json.dumps(rename)
        widget.save_trigger = "go"

        assert json.loads(widget.save_result)["ok"] is True
        assert (
            stream.df.groupby("visitor")["saved"].first().to_dict()
            == shown.map(rename).to_dict()
        )
        assert json.loads(widget.save_params) == snapshot
        assert snapshot["path_col"] == "visitor"
        copied = stream.add_clusters(name="copied", **snapshot)
        assert (
            copied.df.groupby("visitor")["copied"].first().to_dict() == shown.to_dict()
        )

    @pytest.mark.parametrize("updates", [{"features": "[]"}, {"n_clusters": "two"}])
    def test__unsuccessful_apply_invalidates_the_save_snapshot(self, updates) -> None:
        stream, widget = self._widget()
        for key, value in updates.items():
            setattr(widget, key, value)
        widget.apply_trigger = "bad"
        assert widget.result == "{}"
        assert widget.chosen_params == "{}"
        assert widget.save_params == "{}"
        widget.save_segment_name = "saved"
        widget.save_trigger = "go"
        assert json.loads(widget.save_result)["ok"] is False
        assert "saved" not in stream.df.columns

    def test__apply_replaces_the_snapshot_with_the_new_pipeline(self) -> None:
        stream, widget = self._widget()
        old = widget._cluster_labels.copy()
        widget.features = json.dumps([{"metric": "duration"}])
        widget.scaler = ""
        widget.apply_trigger = "duration"
        assert widget.error == ""
        snapshot = json.loads(widget.save_params)
        assert snapshot["features"] == [{"metric": "duration"}]
        assert snapshot["scaler"] is None
        assert not widget._cluster_labels.equals(old)
        copied = stream.add_clusters(name="copied", **snapshot)
        assert (
            copied.df.groupby("visitor")["copied"].first().to_dict()
            == widget._cluster_labels.to_dict()
        )

    def test__headless_widget_tool_does_not_replace_the_displayed_snapshot(
        self,
    ) -> None:
        _, widget = self._widget()
        snapshot = widget.save_params
        chosen = widget.chosen_params
        labels = widget._cluster_labels.copy()
        result = widget._tool_cluster_analysis_data(
            {"features": [{"metric": "duration"}], "n_clusters": 2}
        )
        assert result["overview"]
        assert widget.save_params == snapshot
        assert widget.chosen_params == chosen
        pd.testing.assert_series_equal(widget._cluster_labels, labels)
