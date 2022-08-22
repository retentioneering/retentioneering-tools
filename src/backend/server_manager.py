from __future__ import annotations

import json
from typing import Any

from src.backend import JupyterServer


class Singleton:
    _instances = {}

    def __call__(cls, *args, **kwargs) -> 'ServerManager':
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls, *args, **kwargs)
        return cls._instances[cls]


class ServerManager(metaclass=Singleton):
    _servers: dict[str, JupyterServer]

    def __init__(self) -> None:
        self._servers: dict[str, JupyterServer] = {}
        self._main_listener_created = False

    def _find_server(self, server_id: str) -> JupyterServer | None:
        return self._servers.get(server_id, None)

    def _on_colab_func_called(self, server_id: str, method: str, request_id: str, payload):
        target_server: JupyterServer | None = self._find_server(server_id)
        try:
            if target_server is not None:
                result = target_server.dispatch_method(
                    method=method, payload=payload
                )
                return json.dumps({
                    "success": True,
                    "server_id": server_id,
                    "request_id": request_id,
                    "method": method,
                    "result": result
                })
            else:
                raise Exception('server not found!')
        except Exception as err:
            return json.dumps({
                "success": False,
                "server_id": server_id,
                "request_id": request_id,
                "method": method,
                "result": str(err)
            })

    def _on_comm_message(self, comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            data: dict[str, Any] = msg['content']['data']
            server_id = data['server_id']
            request_id = data['request_id']
            method = data['method']
            payload = data['payload']

            target_server = self._find_server(server_id)
            try:
                if target_server is not None:
                    result = target_server.dispatch_method(
                        method=method, payload=payload)
                    comm.send({
                        "success": True,
                        "server_id": server_id,
                        "request_id": request_id,
                        "method": method,
                        "result": result
                    })
                else:
                    raise Exception('server not found!')
            except Exception as err:
                comm.send({
                    "success": False,
                    "server_id": server_id,
                    "request_id": request_id,
                    "method": method,
                    "result": str(err),
                })

    def _create_main_listener(self):
        if self._main_listener_created:
            return False

        env = self.check_env()

        if env == "colab":
            import google.colab.output
            google.colab.output.register_callback(
                'JupyterServerMainCallback',
                lambda server_id, method, request_id, payload: self._on_colab_func_called(server_id, method, request_id,
                                                                                          payload)
            )
        if env == "classic":
            from IPython import get_ipython
            get_ipython().kernel.comm_manager.register_target(
                'JupyterServerMainCallback',
                lambda comm, open_msg: self._on_comm_message(comm, open_msg))
        self._main_listener_created = True

    def check_env(self) -> str:
        try:
            import google.colab
            return "colab"
        except ImportError:
            return "classic"

    def create_server(self, pk: str = None) -> JupyterServer:
        server = JupyterServer(pk=pk)
        self._create_main_listener()
        self._servers[pk] = server
        return server
