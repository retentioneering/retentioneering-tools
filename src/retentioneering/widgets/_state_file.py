"""Widget state persistence — the ``state_file`` argument.

Every widget accepts a ``state_file`` path that binds its full state (data
and display parameters, plus widget-specific extras like the transition
graph's node layout) to a JSON file: if the file exists, the state is loaded
from it; if not, it is created. Every subsequent state change is auto-saved
to the file. Explicitly passed constructor arguments override the loaded
state (and the overridden values are saved back on the initial snapshot).

The file stores raw traitlet values under a ``state`` key, plus the
``widget_type`` so a file can't silently be reused across widget kinds.
"""

from __future__ import annotations

import json
import os
import pathlib


class StateFileMixin:
    """Adds ``state_file`` persistence to a widget.

    The widget class defines ``_persist_names`` (the traitlet names that make
    up its persistent state) and calls, in this order inside ``__init__``:

    1. ``_load_state_file(state_file)`` — right after ``super().__init__``;
    2. ``_apply_saved_state(exclude=...)`` — after traitlets are set from
       args/defaults, passing the names of explicitly-supplied arguments;
    3. ``_start_state_autosave()`` — last, once observers are safe to attach;
       writes the initial snapshot (creating the file if it doesn't exist).
    """

    _persist_names: tuple[str, ...] = ()

    def _load_state_file(self, state_file) -> None:
        self._state_file = pathlib.Path(state_file).expanduser() if state_file else None
        self._saved_state: dict = {}
        if self._state_file is None or not self._state_file.exists():
            return
        try:
            payload = json.loads(self._state_file.read_text())
        except (OSError, ValueError) as exc:
            raise ValueError(
                f"state_file {self._state_file} is not a valid widget state file: {exc}"
            ) from exc
        saved_type = payload.get("widget_type")
        if saved_type != self.widget_type:
            raise ValueError(
                f"state_file {self._state_file} was saved by a {saved_type!r} "
                f"widget and can't be loaded into a {self.widget_type!r} widget"
            )
        state = payload.get("state") or {}
        self._saved_state = {k: v for k, v in state.items() if k in self._persist_names}

    def _apply_saved_state(self, exclude=()) -> None:
        for name, value in self._saved_state.items():
            if name not in exclude:
                setattr(self, name, value)

    def _start_state_autosave(self) -> None:
        if self._state_file is None:
            return
        self.observe(self._save_state, names=list(self._persist_names))
        self._save_state()

    def _save_state(self, _change=None) -> None:
        payload = {
            "widget_type": self.widget_type,
            "state": {name: getattr(self, name) for name in self._persist_names},
        }
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename so a crash mid-write can't corrupt the state file.
        tmp = self._state_file.with_suffix(self._state_file.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        os.replace(tmp, self._state_file)
