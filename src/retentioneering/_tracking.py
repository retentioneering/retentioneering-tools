"""PostHog analytics tracking for retentioneering."""

import contextvars
import functools
import json
import os
import pathlib
import platform
import sys
import uuid

_KEY = "phc_sev8i3cCtComzsYJJ7KLfYYMqUCSAS49bVep8mkyoKYn"
_HOST = "https://eu.i.posthog.com"
_CONFIG = pathlib.Path.home() / ".retentioneering" / "config.json"

try:
    from posthog import Posthog

    _ph = Posthog(project_api_key=_KEY, host=_HOST)
except Exception:
    _ph = None  # type: ignore[assignment]


# ── distinct_id ────────────────────────────────────────────────────────────────


def _distinct_id() -> tuple[str, str]:
    """Return (distinct_id, id_type)."""
    # 1. /etc/machine-id — Linux (systemd), stable even in most containers
    try:
        mid = pathlib.Path("/etc/machine-id").read_text().strip()
        if mid:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, mid)), "machine-id"
    except Exception:
        pass
    # 2. MAC address — macOS, Windows, Linux without /etc/machine-id
    try:
        node = uuid.getnode()
        # Real MAC address has the multicast bit unset
        if not (node >> 40) & 1:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(node))), "mac-address"
    except Exception:
        pass
    # 3. Saved random UUID — Colab VMs, Docker, anything else
    try:
        _CONFIG.parent.mkdir(parents=True, exist_ok=True)
        if _CONFIG.exists():
            data = json.loads(_CONFIG.read_text())
            if did := data.get("distinct_id"):
                return did, "saved-uuid"
        did = str(uuid.uuid4())
        existing = json.loads(_CONFIG.read_text()) if _CONFIG.exists() else {}
        existing["distinct_id"] = did
        _CONFIG.write_text(json.dumps(existing))
        return did, "saved-uuid"
    except Exception:
        return "anonymous", "anonymous"


# ── environment detection ──────────────────────────────────────────────────────


def _detect_env() -> str:
    if "google.colab" in sys.modules or os.environ.get("COLAB_BACKEND_VERSION"):
        return "colab"
    try:
        shell = get_ipython().__class__.__name__  # type: ignore[name-defined]
        if "ZMQInteractiveShell" in shell:
            if os.environ.get("VSCODE_PID") or os.environ.get("VSCODE_INJECTION"):
                return "vscode"
            return "jupyter"
    except NameError:
        pass
    return "script"


def _kernel_id() -> str | None:
    try:
        import ipykernel

        conn = ipykernel.get_connection_file()
        basename = pathlib.Path(conn).stem  # e.g. "kernel-abc123"
        return basename.replace("kernel-", "")
    except Exception:
        return None


# ── cached base properties ─────────────────────────────────────────────────────


def _lib_version() -> str:
    try:
        from importlib.metadata import version

        return version("retentioneering")
    except Exception:
        return "unknown"


_DISTINCT_ID, _DISTINCT_ID_TYPE = _distinct_id()

_BASE_PROPS: dict = {
    "lib_version": _lib_version(),
    "os": platform.system(),
    "os_version": platform.release(),
    "python_version": platform.python_version(),
    "env": _detect_env(),
    "distinct_id_type": _DISTINCT_ID_TYPE,
}

_kernel = _kernel_id()
if _kernel:
    _BASE_PROPS["kernel_id"] = _kernel


# ── opt-out ────────────────────────────────────────────────────────────────────


def _no_track_requested() -> bool:
    if os.environ.get("RETENTIONEERING_NO_TRACK"):
        return True
    try:
        from google.colab import userdata  # type: ignore[import]

        # Raises SecretNotFoundError for users without the secret — no popup shown.
        if userdata.get("RETENTIONEERING_NO_TRACK"):
            return True
    except Exception:
        pass
    try:
        if json.loads(_CONFIG.read_text()).get("RETENTIONEERING_NO_TRACK"):
            return True
    except Exception:
        pass
    return False


# ── caller context ─────────────────────────────────────────────────────────────

# Set to "mcp" inside MCP tool calls; defaults to "user" everywhere else.
_caller_type: contextvars.ContextVar[str] = contextvars.ContextVar(
    "retentioneering_caller_type", default="user"
)


# ── public API ─────────────────────────────────────────────────────────────────

_depth = 0  # global call depth counter — suppresses nested tracked calls


def track(event: str, properties: dict | None = None) -> None:
    """Fire-and-forget PostHog event. Never raises."""
    if _ph is None or _no_track_requested():
        return
    props = {**_BASE_PROPS, "caller_type": _caller_type.get(), **(properties or {})}
    try:
        _ph.capture(distinct_id=_DISTINCT_ID, event=event, properties=props)
    except Exception:
        pass


def identify(properties: dict | None = None) -> None:
    """Fire-and-forget PostHog identify. Respects the no-track opt-out. Never raises."""
    if _ph is None or _no_track_requested():
        return
    try:
        _ph.identify(_DISTINCT_ID, properties=properties or {})
    except Exception:
        pass


def tracked(event_name: str, condition=None, props_fn=None):
    """Decorator that tracks a method call only when not inside another tracked call.

    condition: optional callable(self) → bool; if False, skip tracking but still execute.
    props_fn:  optional callable(self) → dict; called after execution to collect properties.
               Tracking fires after the method succeeds so props_fn has access to the result.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global _depth
            skip = _depth > 0 or (condition is not None and not condition(args[0]))
            if skip:
                return func(*args, **kwargs)
            _depth += 1
            try:
                result = func(*args, **kwargs)
                props = props_fn(args[0]) if props_fn else None
                track(event_name, props)
                return result
            finally:
                _depth -= 1

        wrapper._tracked_event = event_name  # noqa: SLF001 -- introspected by tests/tracking_test.py
        return wrapper

    return decorator
