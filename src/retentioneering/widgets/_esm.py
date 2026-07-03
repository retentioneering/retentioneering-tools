"""Resolve the widget.js bundle for anywidget.

The JS bundle is built in CI and shipped as package data inside the wheel —
there is no runtime download. In a source checkout without a build, run
`make build` (or `npm run build` in js/widget) first.
"""
import pathlib

_STATIC = pathlib.Path(__file__).parent.parent / "static"


def _get_esm() -> pathlib.Path:
    local = _STATIC / "widget.js"
    if not local.exists():
        raise FileNotFoundError(
            f"{local} not found. Build the JS bundle first: `make build` "
            "(or `npm run build` in js/widget)."
        )
    return local
