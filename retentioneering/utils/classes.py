from typing import Any, List


def call_if_implemented(obj: Any, method_name: str, args: List[Any]) -> Any:
    method = getattr(obj, method_name, None)
    if method is None or not callable(method):
        return None
    try:
        return method(*args)
    except NotImplementedError:
        return None
