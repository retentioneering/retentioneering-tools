from __future__ import annotations

import json
import uuid


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


class Action:
    def __init__(self, method: str, callback):
        self.method = method
        self.callback = callback


class JupyterServer:
    def __init__(self, id: str = None):
        self.id = id if id is not None else self.make_id()
        self.actions = []

    def _find_action(self, method: str):
        for a in self.actions:
            if a.method == method:
                return a
        return None

    def make_id(self):
        return str(uuid.uuid4())

    def use(self, method: str, callback):
        self.actions.append(Action(method, callback))

    def dispatch_method(self, method: str, payload):
        action = self._find_action(method)
        if action is not None:
            return action.callback(payload)
        else:
            raise Exception('method not found!')


@singleton
class ServerManager:
    def __init__(self):
        self._servers = []
        self._main_listener_created = False

    def _find_server(self, server_id: str):
        for server in self._servers:
            if server.id == server_id:
                return server
        return None

    def _on_colab_func_called(self, server_id: str, method: str, request_id: str, payload):
        target_server = self._find_server(server_id)
        try:
            if target_server is not None:
                result = target_server.dispatch_method(
                    method=method, payload=payload)
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
            data = msg['content']['data']
            server_id = data['server_id']
            request_id = data['request_id']
            method = data['method']
            payload = data['payload']

            target_server = self._find_server(server_id)
            try:
                if (target_server is not None):
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
            google.colab.output.register_callback('JupyterServerMainCallback', lambda server_id, method,
                                                                                      request_id,
                                                                                      payload: self._on_colab_func_called(
                server_id, method, request_id, payload))
        if env == "classic":
            from IPython import get_ipython
            get_ipython().kernel.comm_manager.register_target('JupyterServerMainCallback',
                                                              lambda comm, open_msg: self._on_comm_message(comm,
                                                                                                           open_msg))
        self._main_listener_created = True

    def check_env(self):
        try:
            import google.colab
            return "colab"
        except ImportError:
            return "classic"

    def create_server(self, id: str = None):
        server = JupyterServer(id=id)
        self._create_main_listener()
        self._servers.append(server)
        return server
