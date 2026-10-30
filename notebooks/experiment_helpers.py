"""Presentation helpers only; statistical calculations live in Retentioneering."""

import html
from pathlib import Path

from IPython.display import HTML, display


def result_from_payload(payload):
    """Notebook table/plot adapter; all computation lives in Eventstream."""
    import copy
    import json
    import pandas as pd

    class View:
        def __init__(self, value):
            self._payload = copy.deepcopy(value)
            self.summary = pd.DataFrame(value["summary"])
            self.diagnostics = pd.DataFrame(value["diagnostics"])
            self.metadata = copy.deepcopy(value["metadata"])

        def to_dict(self):
            return copy.deepcopy(self._payload)

        def to_json(self, path):
            Path(path).write_text(
                json.dumps(self._payload, indent=2, allow_nan=False) + "\n"
            )

    return View(payload)


def show_native(widget, output, filename, title, analysis):
    path = Path(output) / filename
    widget.export_html(str(path), title=title, analysis=analysis)
    content = html.escape(path.read_text(), quote=True)
    display(
        HTML(
            f'<iframe title="{html.escape(title, quote=True)}" srcdoc="{content}" style="width:100%;height:520px;border:1px solid #ddd;border-radius:6px"></iframe>'
        )
    )
    return path


def save_figure(fig, output, name):
    for extension in ("svg", "png"):
        fig.savefig(
            Path(output) / f"{name}.{extension}",
            dpi=150,
            bbox_inches="tight",
            facecolor="white",
        )
