from __future__ import annotations

import uuid

from src.exceptions.server import ServerNotFoundActionError


class Action:
    def __init__(self, method: str, callback: callable):
        self.method = method
        self.callback = callback


class JupyterServer:
    pk: str
    actions: dict[str, Action]

    def __init__(self, pk: str = None):
        self.pk = pk if pk is not None else self.make_id()
        self.actions: dict[str, Action] = {}

    def _find_action(self, method: str) -> None | Action:
        return self.actions.get(method, None)

    def make_id(self) -> str:
        return str(uuid.uuid4())

    def register_action(self, method: str, callback: callable) -> None:
        self.actions[method] = Action(method, callback)

    def dispatch_method(self, method: str, payload: dict):
        action = self._find_action(method)
        if action is not None:
            return action.callback(**payload)
        else:
            raise ServerNotFoundActionError('method not found!', method=method)
