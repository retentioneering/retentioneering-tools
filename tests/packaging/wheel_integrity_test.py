"""Negative controls for the actual pre-publication wheel guard."""

import zipfile
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "verify_wheel.py"
spec = spec_from_file_location("verify_wheel", SCRIPT)
verify = module_from_spec(spec)
spec.loader.exec_module(verify)


@pytest.fixture
def packaged(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "pyproject.toml").write_text(
        '[project]\nname = "retentioneering"\nversion = "5.4.0"\n'
    )
    contents = {
        "retentioneering/__init__.py": b"# package\n",
        **{asset: b"required asset\n" for asset in verify.REQUIRED_ASSETS},
    }
    for name, data in contents.items():
        path = source / "src" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def build(*, missing=None, changed=None, version="5.4.0"):
        wheel = tmp_path / "candidate.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            for name, data in contents.items():
                if name != missing:
                    archive.writestr(name, b"wrong bytes" if name == changed else data)
            archive.writestr(
                "retentioneering-5.4.0.dist-info/METADATA",
                f"Name: retentioneering\nVersion: {version}\n",
            )
        return wheel

    return source, build


def test_complete_artifact_matches_source(packaged):
    source, build = packaged
    result = verify.verify_artifact(build(), source)
    assert set(result["source_package_sha256"]) == {
        "retentioneering/__init__.py",
        *verify.REQUIRED_ASSETS,
    }
    assert result["version"] == "5.4.0"


@pytest.mark.parametrize("asset", verify.REQUIRED_ASSETS)
def test_each_missing_runtime_asset_is_rejected(packaged, asset):
    source, build = packaged
    with pytest.raises(ValueError, match="required wheel asset"):
        verify.verify_artifact(build(missing=asset), source)


def test_missing_package_code_is_rejected(packaged):
    source, build = packaged
    with pytest.raises(ValueError, match="source package file missing"):
        verify.verify_artifact(build(missing="retentioneering/__init__.py"), source)


def test_different_source_bytes_are_rejected(packaged):
    source, build = packaged
    with pytest.raises(ValueError, match="bytes differ"):
        verify.verify_artifact(
            build(changed="retentioneering/static/widget.js"), source
        )


def test_old_distribution_version_is_rejected(packaged):
    source, build = packaged
    with pytest.raises(ValueError, match="name/version"):
        verify.verify_artifact(build(version="5.3.0"), source)
