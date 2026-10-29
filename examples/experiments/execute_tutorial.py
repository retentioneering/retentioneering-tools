"""Execute with this Python environment, then export a readable HTML report."""

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import nbformat
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager
from nbclient import NotebookClient
from nbconvert import HTMLExporter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lumeo", action="store_true")
    args = parser.parse_args()
    name = "lumeo_experiment_tutorial" if args.lumeo else "experiment_tutorial"
    root = Path(__file__).resolve().parents[2] / "notebooks"
    output = root.parent / "artifacts" / name
    output.mkdir(parents=True, exist_ok=True)
    os.environ["LUMEO_AB_OUTPUT_DIR"] = str(output)
    notebook = nbformat.read(root / f"{name}.ipynb", as_version=4)
    start = time.monotonic()
    os.environ["RETENTIONEERING_NO_TRACK"] = "1"
    with tempfile.TemporaryDirectory(prefix="lumeo-ab-kernel-") as temporary:
        temp = Path(temporary)
        spec = temp / "kernels" / "lumeo-ab"
        spec.mkdir(parents=True)
        (spec / "kernel.json").write_text(
            json.dumps(
                {
                    "argv": [
                        sys.executable,
                        "-m",
                        "ipykernel_launcher",
                        "-f",
                        "{connection_file}",
                    ],
                    "display_name": "Lumeo A/B (current environment)",
                    "language": "python",
                    "env": {
                        "RETENTIONEERING_NO_TRACK": "1",
                        "MPLCONFIGDIR": str(temp / "mpl"),
                        "IPYTHONDIR": str(temp / "ipython"),
                        "OPENBLAS_NUM_THREADS": "1",
                    },
                }
            )
        )
        manager = KernelManager(
            kernel_name="lumeo-ab",
            kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(spec.parent)]),
        )
        client = NotebookClient(
            notebook,
            km=manager,
            timeout=300,
            resources={"metadata": {"path": str(root)}},
        )
        try:
            client.execute(cwd=str(root))
        except Exception:
            nbformat.write(notebook, output / f"{name}.failed.ipynb")
            raise
        finally:
            manager.shutdown_kernel(now=True)
    nbformat.write(notebook, output / f"{name}.executed.ipynb")
    exporter = HTMLExporter()
    exporter.exclude_input_prompt = True
    exporter.exclude_output_prompt = True
    body, _ = exporter.from_notebook_node(notebook)
    (output / f"{name}.html").write_text(body)
    summary = {
        "python": sys.version,
        "elapsed_seconds": round(time.monotonic() - start, 2),
        "code_cells_executed": sum(c.cell_type == "code" for c in notebook.cells),
        "cell_errors": sum(
            o.output_type == "error"
            for c in notebook.cells
            if c.cell_type == "code"
            for o in c.outputs
        ),
    }
    (output / "execution.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
