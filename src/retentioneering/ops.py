"""
Serializable operation model for `Eventstream` processors.

An "op" is a plain JSON-serializable dict: `{"type": "<processor_method_name>",
**params}` — one record per processor invocation, naming the `Eventstream`
method that produced a derived stream and the (non-default) arguments it was
called with. This is exactly the shape `mcp/server.py`'s `_apply_preprocessors`
already used for its `preprocessors` step lists; this module elevates that
ad hoc format into the library's official, documented representation instead
of reinventing it. MCP now delegates to `apply_ops()` below (see
`_apply_preprocessors`).

Three things live here:

- `Op` — a tiny dataclass wrapper with `to_dict()`/`from_dict()`, for callers
  who want a typed single-op value instead of a bare dict.
- `op` — the decorator applied to every processor method on `Eventstream`
  (see `eventstream.py`). Wrapping each method here, once, is the "one wiring
  point" for lineage recording: `Eventstream` has no central processor-dispatch
  funnel to hook into (every processor method independently builds its own
  `DataProcessor` and rewraps the result — see ADR-0003), so decorating each
  method definition is the highest-leverage single hook available without
  editing N method bodies by hand.
- `apply_op` / `apply_ops` — replay a single op / an ordered list of ops
  against a base `Eventstream` by dispatching `op["type"]` to the
  same-named `Eventstream` method with `**params`.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from functools import wraps
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


# ── op record ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Op:
    """A single, typed op record. Most of this module works with plain dicts
    (that's the on-the-wire/JSON shape, and what `Eventstream._lineage` and
    `Eventstream.recipe()` store) — `Op` is a convenience wrapper for callers
    who'd rather not hand-roll dict shuffling."""

    type: str
    params: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Flatten back to the `{"type": ..., **params}` wire shape."""
        return {"type": self.type, **self.params}

    @classmethod
    def from_dict(cls, d: dict) -> "Op":
        d = dict(d)
        op_type = d.pop("type", None)
        if not op_type:
            raise ValueError(f"op dict missing required 'type' key: {d!r}")
        return cls(type=op_type, params=d)


# ── registry (populated by the `op` decorator as Eventstream methods are
# defined; by the time apply_op/apply_ops run at call time, every decorated
# method has already registered itself) ──────────────────────────────────────

_REGISTERED_OPS: set[str] = set()


def registered_ops() -> frozenset[str]:
    """Names of every `Eventstream` method currently wired for lineage
    recording — the complete processor surface `apply_op`/`apply_ops` can
    dispatch to."""
    return frozenset(_REGISTERED_OPS)


def _is_default(value: Any, default: Any) -> bool:
    """Best-effort equality check that never raises (e.g. for callables/arrays,
    where `value == default` can be ambiguous or raise)."""
    if value is default:
        return True
    try:
        return bool(value == default)
    except Exception:
        return False


def op(func):
    """Decorator for `Eventstream` processor methods: records lineage.

    After `func` runs, if it returned an object with a `_lineage` attribute
    (i.e. another `Eventstream`), sets:

        result._lineage = self._lineage + [{"type": func.__name__, **params}]

    where `params` is the subset of `func`'s bound arguments that were passed
    and differ from their declared default — the same minimal shape MCP's
    preprocessor steps use. This intentionally *overwrites* whatever lineage
    the returned object already carried: a few processors internally call
    another decorated processor method (e.g. `filter_paths` calls
    `filter_events`) as an implementation detail, and the outer call is the
    one op that should show up in the chain, not the inner one.
    """
    sig = inspect.signature(func)
    op_type = func.__name__
    _REGISTERED_OPS.add(op_type)

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if hasattr(result, "_lineage"):
            try:
                bound = sig.bind(self, *args, **kwargs)
            except TypeError:
                params: dict = {}
            else:
                params = {}
                for pname, pval in bound.arguments.items():
                    if pname == "self":
                        continue
                    param = sig.parameters[pname]
                    if param.kind in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    ):
                        continue
                    default = param.default
                    if default is not inspect.Parameter.empty and _is_default(
                        pval, default
                    ):
                        continue
                    params[pname] = pval
            result._lineage = list(self._lineage) + [{"type": op_type, **params}]
        return result

    wrapper._op_type = op_type  # noqa: SLF001 -- introspected by tests
    return wrapper


# ── dispatch / replay ─────────────────────────────────────────────────────────


def _adapt_params(op_type: str, params: dict) -> dict:
    """Shape adapters for ops whose on-the-wire dict doesn't map 1:1 onto the
    matching method's keyword arguments.

    `filter_paths` is the one processor that doesn't fit the common
    `{"type": ..., **kwargs}` -> `method(**kwargs)` pattern cleanly: MCP's
    `_apply_preprocessors` (preserved here for backward compatibility, since
    that's still MCP's documented external `preprocessors` step shape) takes
    the condition tree *flattened* directly into the step dict rather than
    nested under a `condition` key, e.g.
    `{"type": "filter_paths", "op": ">", "metric": "length", "value": 3}`.
    Lineage recording (via the `op` decorator above) instead records the
    natural, signature-matching shape with an explicit `condition` key. Both
    are accepted here: if `condition` is already present, this is a no-op.
    """
    if op_type == "filter_paths" and "condition" not in params:
        params = dict(params)
        path_col = params.pop("path_col", None)
        event_col = params.pop("event_col", None)
        adapted = {"condition": params}
        if path_col is not None:
            adapted["path_col"] = path_col
        if event_col is not None:
            adapted["event_col"] = event_col
        return adapted
    return params


def apply_op(stream: "Eventstream", op_: dict | Op) -> "Eventstream":
    """Apply a single op (a `{"type": ..., **params}` dict, or an `Op`) to
    `stream` by dispatching to the same-named `Eventstream` method."""
    if isinstance(op_, Op):
        op_dict = op_.to_dict()
    else:
        op_dict = dict(op_)

    op_type = op_dict.pop("type", None)
    if not op_type:
        raise ValueError(f"op dict missing required 'type' key: {op_dict!r}")

    method = getattr(stream, op_type, None)
    if not callable(method) or op_type not in registered_ops():
        raise ValueError(
            f"Unknown or non-processor op type: {op_type!r}. "
            f"Supported: {', '.join(sorted(registered_ops()))}."
        )

    params = _adapt_params(op_type, op_dict)
    return method(**params)


def apply_ops(stream: "Eventstream", ops: list[dict | Op]) -> "Eventstream":
    """Apply an ordered list of ops to `stream`, in order, returning the final
    `Eventstream`. Used by `Eventstream.from_recipe()` and by
    `mcp/server.py`'s `_apply_preprocessors`."""
    for op_ in ops:
        stream = apply_op(stream, op_)
    return stream
