"""Cloud storage for widget states.

No cloud backend is bundled with this open-source distribution. Set
RETENTIONEERING_CLOUD_SUPABASE_URL / RETENTIONEERING_CLOUD_SUPABASE_ANON_KEY
to point this at a Supabase project running the schema in supabase/widget_states.sql.
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

_SUPABASE_URL = os.environ.get("RETENTIONEERING_CLOUD_SUPABASE_URL", "")
_SUPABASE_ANON_KEY = os.environ.get("RETENTIONEERING_CLOUD_SUPABASE_ANON_KEY", "")
_TABLE = "widget_states"
_TIMEOUT = 10


def _user_id_from_token(token: str) -> str:
    import base64
    part = token.split(".")[1]
    part += "=" * (4 - len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(part))["sub"]


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "apikey": _SUPABASE_ANON_KEY,
    }


def save(token: str, file_name: str, widget_type: str, state: dict) -> None:
    url = f"{_SUPABASE_URL}/rest/v1/{_TABLE}?on_conflict=user_id,object_name"
    payload = json.dumps({
        "user_id":     _user_id_from_token(token),
        "object_name": file_name,
        "widget_type": widget_type,
        "state":       state,
    }).encode()
    req = urllib.request.Request(url, data=payload, method="POST")
    for k, v in _headers(token).items():
        req.add_header(k, v)
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "resolution=merge-duplicates,return=minimal")
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT):
            pass
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Cloud save failed (HTTP {exc.code}): {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cloud save failed (network error): {exc.reason}") from exc


def exists(token: str, file_name: str) -> bool:
    """Return True if a saved state with this name exists for the authenticated user."""
    params = urllib.parse.urlencode({
        "object_name": f"eq.{file_name}",
        "select":      "object_name",
        "limit":       "1",
    })
    url = f"{_SUPABASE_URL}/rest/v1/{_TABLE}?{params}"
    req = urllib.request.Request(url)
    for k, v in _headers(token).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return len(json.loads(resp.read().decode())) > 0
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Cloud check failed (HTTP {exc.code}): {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cloud check failed (network error): {exc.reason}") from exc


def load(token: str, file_name: str) -> dict | None:
    params = urllib.parse.urlencode({
        "object_name": f"eq.{file_name}",
        "select":      "state",
        "limit":       "1",
    })
    url = f"{_SUPABASE_URL}/rest/v1/{_TABLE}?{params}"
    req = urllib.request.Request(url)
    for k, v in _headers(token).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
            return data[0]["state"] if data else None
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Cloud load failed (HTTP {exc.code}): {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cloud load failed (network error): {exc.reason}") from exc
