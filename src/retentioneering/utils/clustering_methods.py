"""
Per-method argument schema shared by the three clustering surfaces
(`add_clusters`, `cluster_analysis_data`, `cluster_analysis`).

`method` names an algorithm and `method_args` carries that algorithm's own
parameters — the `metric` / `metric_args` shape, applied to a registry of
algorithms instead of a registry of metrics (ADR-0008, rule 8). Keeping the
schema in one place is what makes "a key that does not belong to `method`" an
error rather than a silently dropped argument, which is how the flat
`n_clusters=` / `min_cluster_size=` signature used to behave.

`scaler` and `nmf_components` are deliberately *not* here: they are pipeline
steps applied before clustering, valid for every method.
"""

from typing import Any, Dict

METHOD_ARGS: Dict[str, tuple] = {
    "kmeans": ("n_clusters",),
    "hdbscan": ("min_cluster_size", "cluster_selection_epsilon"),
}


def parse_method_args(
    method: str,
    method_args: Dict[str, Any] | None,
    *,
    error,
) -> Dict[str, Any]:
    """
    Validate `method_args` against `method` and return it as a plain dict.

    `error` builds the exception to raise, so each caller reports in its own
    dialect (`PreprocessingConfigError` for the data processor, `ValueError`
    for the headless tool).
    """
    if method not in METHOD_ARGS:
        raise error(
            f"Unknown clustering method: {method!r}. "
            f"Use one of {sorted(METHOD_ARGS)}."
        )

    allowed = METHOD_ARGS[method]
    if method_args is None:
        return {}
    if not isinstance(method_args, dict):
        raise error(
            f"'method_args' must be a dict, got {type(method_args).__name__}. "
            f"For method {method!r} its keys are {list(allowed)}."
        )

    unknown = [k for k in method_args if k not in allowed]
    if unknown:
        owners = {key: name for name, keys in METHOD_ARGS.items() for key in keys}
        hints = [
            f"{k!r} belongs to the {owners[k]!r} method"
            if k in owners
            else f"{k!r} is not a clustering parameter"
            for k in unknown
        ]
        raise error(
            f"method_args key(s) {unknown} are not parameters of the {method!r} "
            f"method, which takes {list(allowed)} ({'; '.join(hints)})."
        )

    return dict(method_args)
