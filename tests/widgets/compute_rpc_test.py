"""Tests for the compute_request/compute_response RPC in RetentioneeringWidget.

See https://github.com/retentioneering/retentioneering-tools/issues/175.
"""

import json
import math

import numpy as np
import pytest

from retentioneering.widgets._base import RetentioneeringWidget


def _tool_with_nan(self, params: dict):
    return {
        "mean": float("nan"),
        "max": math.inf,
        "rows": [{"value": np.float64("nan")}, (1.5, -math.inf)],
        "count": 3,
    }


def _tool_unserializable(self, params: dict):
    return {"value": object()}


def _tool_interrupted(self, params: dict):
    raise KeyboardInterrupt


class _Widget(RetentioneeringWidget):
    compute_tools = {
        "with_nan": _tool_with_nan,
        "unserializable": _tool_unserializable,
        "interrupted": _tool_interrupted,
    }


def _request(widget: RetentioneeringWidget, tool: str) -> dict:
    widget.compute_request = json.dumps({"id": "req-1", "tool": tool, "params": {}})
    return json.loads(widget.compute_response)


class TestComputeRpc:
    def test__non_finite_floats_come_back_as_null(self) -> None:
        response = _request(_Widget(), "with_nan")

        assert response == {
            "id": "req-1",
            "result": {
                "mean": None,
                "max": None,
                "rows": [{"value": None}, [1.5, None]],
                "count": 3,
            },
        }

    def test__dispatch_compute_returns_strict_json(self) -> None:
        result = _Widget().dispatch_compute("with_nan", {})

        json.dumps(result, allow_nan=False)

    def test__unserializable_result_becomes_an_error_response(self) -> None:
        response = _request(_Widget(), "unserializable")

        assert response["id"] == "req-1"
        assert "error" in response

    def test__unknown_tool_becomes_an_error_response(self) -> None:
        response = _request(_Widget(), "missing")

        assert response == {"id": "req-1", "error": "Unknown tool: 'missing'"}

    def test__interrupt_still_answers_the_request(self) -> None:
        widget = _Widget()

        with pytest.raises(KeyboardInterrupt):
            widget.compute_request = json.dumps(
                {"id": "req-1", "tool": "interrupted", "params": {}}
            )

        assert json.loads(widget.compute_response) == {
            "id": "req-1",
            "error": "Computation interrupted",
        }
