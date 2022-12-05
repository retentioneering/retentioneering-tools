from __future__ import annotations

import json
from typing import Any, Optional

from ipykernel.comm.comm import Comm

from src.backend import JupyterServer
from src.utils.singleton import Singleton


class ServerManager:
    __metaclass__ = Singleton
    _servers: dict[str, JupyterServer]

    def __init__(self) -> None:
        self._servers: dict[str, JupyterServer] = {}
        self._main_listener_created = False

    def _find_server(self, server_id: str) -> JupyterServer | None:
        return self._servers.get(server_id, None)

    def _on_colab_func_called(self, server_id: str, method: str, request_id: str, payload: dict) -> str:
        target_server: JupyterServer | None = self._find_server(server_id)
        if target_server is None:
            err = "ServerNotFound"
            return json.dumps(
                {
                    "success": False,
                    "server_id": server_id,
                    "request_id": request_id,
                    "method": method,
                    "result": str(err),
                }
            )
        result = target_server.dispatch_method(method=method, payload=payload)
        return json.dumps(
            {
                "success": True,
                "server_id": server_id,
                "request_id": request_id,
                "method": method,
                "result": result,
            }
        )

    def _on_comm_message(self, comm: Comm, open_msg: Any) -> None:
        @comm.on_msg  # type: ignore
        def _recv(msg: dict[str, dict]) -> None:
            data: dict[str, Any] = msg["content"]["data"]
            server_id = data["server_id"]
            request_id = data["request_id"]
            method = data["method"]
            payload = data.get("payload", {})

            target_server = self._find_server(server_id)
            if target_server is None:
                raise Exception("server not found!")

            try:
                result = target_server.dispatch_method(method=method, payload=payload)
                comm.send(
                    {
                        "success": True,
                        "server_id": server_id,
                        "request_id": request_id,
                        "method": method,
                        "result": result,
                    }
                )
            except Exception as err:
                comm.send(
                    {
                        "success": False,
                        "server_id": server_id,
                        "request_id": request_id,
                        "method": method,
                        "result": str(err),
                    }
                )

    def _create_main_listener(self) -> None:

        env = self.check_env()

        if env == "colab":
            import google.colab.output  # type: ignore

            google.colab.output.register_callback(
                "JupyterServerMainCallback",
                lambda server_id, method, request_id, payload: self._on_colab_func_called(
                    server_id, method, request_id, payload
                ),
            )
        if env == "classic":
            from IPython.core.getipython import get_ipython

            if get_ipython() is not None:
                get_ipython().kernel.comm_manager.register_target(
                    "JupyterServerMainCallback", lambda comm, open_msg: self._on_comm_message(comm, open_msg)
                )
        self._main_listener_created = True

    def check_env(self) -> str:
        try:
            import google.colab  # type: ignore # noqa: F401

            return "colab"
        except ImportError:
            return "classic"

    def create_server(self, pk: Optional[str] = None) -> JupyterServer:
        server = JupyterServer(pk=pk)
        self._create_main_listener()
        self._servers[server.pk] = server
        return server
