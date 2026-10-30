"""Read-only experiment result view with the same payload in Jupyter and HTML."""

import json

import traitlets

from retentioneering.widgets._base import RetentioneeringWidget


class ExperimentWidget(RetentioneeringWidget):
    """Render one immutable calculation from ``Eventstream.experiment_data``.

    Assignment rows and event data stay in Python. Display changes do not
    recompute the experiment or draw another bootstrap sample. This initial
    view does not support state-file restore or interactive data parameters;
    create a new widget to calculate a different specification.
    """

    widget_type = traitlets.Unicode("experiment", read_only=True).tag(sync=True)
    result = traitlets.Unicode("{}", read_only=True).tag(sync=True)
    height = traitlets.Int(650, min=200).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
    _export_label = "Experiment"

    def __init__(
        self,
        eventstream,
        *,
        assignments,
        metrics,
        observation,
        variant_col="exp_group",
        control="control",
        treatment="treatment",
        unit_col=None,
        bootstrap=None,
        funnel_steps=None,
        expected_allocation=None,
        tests=None,
        event_catalog=None,
        binary_intervals=None,
        height=650,
        sidebar_open=True,
    ):
        super().__init__()
        self.height = height
        self.sidebar_open = sidebar_open
        payload = eventstream.experiment_data(
            assignments=assignments,
            metrics=metrics,
            observation=observation,
            variant_col=variant_col,
            control=control,
            treatment=treatment,
            unit_col=unit_col,
            bootstrap=bootstrap,
            funnel_steps=funnel_steps,
            expected_allocation=expected_allocation,
            tests=tests,
            event_catalog=event_catalog,
            binary_intervals=binary_intervals,
        )
        if payload.get("schema_version") != "2.0":
            raise ValueError("ExperimentWidget requires result schema_version='2.0'")
        # The headless result owns JSON normalization. Encoding here both
        # validates that contract and detaches the displayed snapshot from
        # any mutable dictionaries held by the caller or a backend cache.
        self.set_trait("result", json.dumps(payload, allow_nan=False))

    def _export_data(self, sidebar_open: bool | None = None) -> dict:
        return {
            "widget_type": "experiment",
            "result": json.loads(self.result),
            "height": self.height,
            "sidebar_open": self.sidebar_open if sidebar_open is None else sidebar_open,
        }
