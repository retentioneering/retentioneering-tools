#!/usr/bin/env python3
"""Verify source/artifact parity and exercise a wheel in a fresh, isolated environment.

Run after building JavaScript and the wheel. No editable install, source PYTHONPATH,
or publication is involved. The numerical smoke is an artifact check, not a
statistical coverage certificate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from email.parser import BytesParser
from pathlib import Path

REQUIRED_ASSETS = (
    "retentioneering/static/widget.js",
    "retentioneering/static/widget-static.js",
    "retentioneering/static/widget.css",
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_artifact(wheel: Path, source_root: Path) -> dict:
    """Reject absent/empty assets, changed package bytes and wrong source version."""
    source_root = source_root.resolve()
    package = source_root / "src" / "retentioneering"
    match = re.search(
        r'^version\s*=\s*"([^"]+)"',
        (source_root / "pyproject.toml").read_text(),
        re.MULTILINE,
    )
    if match is None:
        raise ValueError("pyproject.toml must declare a literal project version")
    version = match.group(1)
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("wheel has duplicate archive entries")
        for name in REQUIRED_ASSETS:
            if name not in names or not archive.read(name):
                raise ValueError(f"required wheel asset missing or empty: {name}")
        metadata_names = [n for n in names if n.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise ValueError("wheel must contain exactly one distribution METADATA")
        metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
        if metadata["Name"] != "retentioneering" or metadata["Version"] != version:
            raise ValueError("wheel name/version differs from source project metadata")
        source_hashes = {}
        for path in sorted(package.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(package)
            if any(
                part.startswith(".") or part == "__pycache__" for part in relative.parts
            ):
                continue
            if path.suffix in {".pyc", ".pyo"}:
                continue
            name = "retentioneering/" + relative.as_posix()
            source_hashes[name] = sha256(path.read_bytes())
            if name not in names:
                raise ValueError(f"source package file missing from wheel: {name}")
            if sha256(archive.read(name)) != source_hashes[name]:
                raise ValueError(f"wheel bytes differ from source: {name}")
        if "retentioneering/__init__.py" not in source_hashes:
            raise ValueError("source package __init__.py is missing")
        packaged = {
            n for n in names if n.startswith("retentioneering/") and not n.endswith("/")
        }
        if packaged != set(source_hashes):
            raise ValueError(
                f"wheel contains unexpected package files: {sorted(packaged - set(source_hashes))}"
            )
    provenance = {}
    repository = subprocess.run(
        ["git", "-C", str(source_root), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    is_checkout = (
        repository.returncode == 0
        and Path(repository.stdout.strip()).resolve() == source_root
    )
    for label, args in (
        ("commit", ["rev-parse", "HEAD"]),
        ("status", ["status", "--porcelain"]),
    ):
        result = (
            subprocess.run(
                ["git", "-C", str(source_root), *args],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            if is_checkout
            else None
        )
        provenance[label] = (
            result.stdout.strip()
            if result is not None and result.returncode == 0
            else None
        )
    return {
        "wheel": str(wheel.resolve()),
        "wheel_sha256": sha256(wheel.read_bytes()),
        "version": version,
        "source_root": str(source_root),
        "source_git": provenance,
        "source_package_sha256": source_hashes,
        "required_assets": list(REQUIRED_ASSETS),
    }


def run_gate(args) -> dict:
    wheel, output = args.wheel.resolve(), args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest = verify_artifact(wheel, args.source_root)
    manifest_path = output / "wheel-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    with tempfile.TemporaryDirectory(prefix="retentioneering-wheel-") as temp:
        temporary = Path(temp)
        environment = (
            args.environment_dir.resolve()
            if args.environment_dir
            else temporary / "venv"
        )
        if environment.exists():
            raise ValueError(
                f"wheel verification requires a new environment: {environment}"
            )
        subprocess.run(
            [args.uv, "venv", "--python", args.python, str(environment)],
            check=True,
            timeout=120,
        )
        python = environment / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        install = [args.uv, "pip", "install", "--python", str(python)]
        if args.offline:
            install.append("--offline")
        subprocess.run([*install, str(wheel)], check=True, timeout=900)
        subprocess.run(
            [args.uv, "pip", "check", "--python", str(python)], check=True, timeout=120
        )
        smoke = temporary / "installed_wheel_smoke.py"
        shutil.copyfile(Path(__file__).with_name("installed_wheel_smoke.py"), smoke)
        env = dict(os.environ, RETENTIONEERING_NO_TRACK="1")
        env.pop("PYTHONPATH", None)
        env.pop("PYTHONHOME", None)
        subprocess.run(
            [
                str(python),
                "-I",
                str(smoke),
                "--manifest",
                str(manifest_path),
                "--output",
                str(output),
            ],
            cwd=temporary,
            env=env,
            check=True,
            timeout=300,
        )
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument(
        "--source-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/wheel"))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument(
        "--environment-dir",
        type=Path,
        help="Keep a new wheel environment here for further checks",
    )
    args = parser.parse_args()
    manifest = run_gate(args)
    print(f"Verified installed wheel {manifest['wheel_sha256']}")


if __name__ == "__main__":
    main()
