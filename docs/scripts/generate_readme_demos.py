"""Export the real widgets used by the README recordings.

Run from the repository root after installing retentioneering and building its JS:
    RETENTIONEERING_NO_TRACK=1 python docs/scripts/generate_readme_demos.py
Outputs go to docs/build/readme/ (gitignored).
"""

import json
import os
from pathlib import Path

import retentioneering as rete


def main():
    out = Path(__file__).resolve().parents[1] / "build" / "readme"
    out.mkdir(parents=True, exist_ok=True)
    stream = rete.datasets.load_ecom()
    stages = stream.add_segment(
        "stage",
        funnel_events=["cart", "shipping_details", "purchase"],
        path_col="session_id",
    )
    matrix = stages.step_matrix(
        anchor="shipping_details",
        max_steps=6,
        path_col="session_id",
        diff=("stage", "shipping_details", "purchase"),
        sidebar_open=False,
        height=460,
    )
    matrix.export_html(out / "step-matrix.html", title="After the shipping step")
    clusters = stream.cluster_analysis(
        features=[{"metric": "event_count_bulk"}],
        method_args={"n_clusters": "3-5"},
        path_col="session_id",
        sidebar_open=False,
        height=680,
    )
    clusters.export_html(out / "cluster-analysis.html", title="Session behavior types")
    # Execute the maintained, self-contained notebook rather than duplicating its fixture.
    notebook = out.parents[2] / "notebooks" / "agent_path_analysis.ipynb"
    cells = json.loads(notebook.read_text())["cells"]
    namespace = {}
    previous_dir = Path.cwd()
    try:
        os.chdir(out)
        for cell in cells:
            source = "".join(cell["source"])
            if cell["cell_type"] == "code" and not source.startswith("%pip"):
                exec(compile(source, str(notebook), "exec"), namespace)
    finally:
        os.chdir(previous_dir)
    print(f"Exported README widgets to {out}")


if __name__ == "__main__":
    main()
