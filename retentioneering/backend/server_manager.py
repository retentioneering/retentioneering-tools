from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal, Optional

from ipykernel.comm.comm import Comm

from retentioneering.backend import JupyterServer
from retentioneering.exceptions.server import ServerErrorWithResponse
from retentioneering.utils.singleton import Singleton

EnvId = Literal["classic", "colab"]


class ServerManager:
    __metaclass__ = Singleton
    kernel_id: Optional[str]
    _servers: dict[str, JupyterServer]

    def __init__(self) -> None:
        self._servers: dict[str, JupyterServer] = {}
        self._main_listener_created = False
        self.kernel_id = self._get_kernel_id()

    def _get_kernel_id(self) -> str | None:
        try:
            import ipykernel

            connection_file = Path(ipykernel.get_connection_file()).stem
            return connection_file.split("-", 1)[1]
        except:
            return None

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
        try:
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
        except ServerErrorWithResponse as err:
            return json.dumps(
                {
                    "success": False,
                    "server_id": server_id,
                    "request_id": request_id,
                    "method": method,
                    "result": err.dict(),
                }
            )
        except Exception as err:
            wrapped_exc = ServerErrorWithResponse(message=str(err), type="unexpected_error")

            return json.dumps(
                {
                    "success": False,
                    "server_id": server_id,
                    "request_id": request_id,
                    "method": method,
                    "result": wrapped_exc.dict(),
                }
            )

    def _on_comm_message(self, comm: Comm, open_msg: Any) -> None:
        @comm.on_msg  # type: ignore
        def _recv(msg: dict[str, dict]) -> None:
            data: dict[str, Any] = msg["content"]["data"]
            server_id = data["server_id"]
            request_id = data["request_id"]
            request_type = data.get("request_type", None)

            target_server = self._find_server(server_id)
            if target_server is None:
                raise Exception("server not found!")

            if request_type == "handshake":
                comm.send(
                    {"success": True, "server_id": server_id, "request_id": request_id, "request_type": "handshake"}
                )
                return

            method = data["method"]
            payload = data.get("payload", {})

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
            except ServerErrorWithResponse as err:
                comm.send(
                    {
                        "success": False,
                        "server_id": server_id,
                        "request_id": request_id,
                        "method": method,
                        "result": err.dict(),
                    }
                )
            except Exception as err:
                wrapped_exc = ServerErrorWithResponse(message=str(err), type="unexpected_error")

                comm.send(
                    {
                        "success": False,
                        "server_id": server_id,
                        "request_id": request_id,
                        "method": method,
                        "result": wrapped_exc.dict(),
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

            if ipython := get_ipython():
                ipython.kernel.comm_manager.register_target(
                    "JupyterServerMainCallback", lambda comm, open_msg: self._on_comm_message(comm, open_msg)
                )
        self._main_listener_created = True

    def check_env(self) -> EnvId:
        try:
            import google.colab  # type: ignore # noqa: F401

            return "colab"
        except ImportError:
            return "classic"

    def create_server(self, pk: Optional[str] = None) -> JupyterServer:
        server = JupyterServer(pk=pk, kernel_id=self.kernel_id)
        self._create_main_listener()
        self._servers[server.pk] = server
        return server
