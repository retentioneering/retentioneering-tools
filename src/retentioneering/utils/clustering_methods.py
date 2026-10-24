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

k-means also takes its two initialisation arguments here, `n_init` and
`random_state`. They are not grid coordinates (a list is not searched over) but
the way to make a k-means split reproducible across seeds: scikit-learn's
default `n_init="auto"` runs k-means++ once, and on features with a long tail
that single run lands in a different local optimum from seed to seed.
"""

from typing import Any, Dict

METHOD_ARGS: Dict[str, tuple] = {
    "kmeans": ("n_clusters", "n_init", "random_state"),
    "hdbscan": ("min_cluster_size", "cluster_selection_epsilon"),
}

# The k-means initialisation keys and the scikit-learn defaults used when they are
# absent. Kept apart from `n_clusters` so the grid search and `best_params` can
# tell a coordinate from a setting that is merely carried along.
KMEANS_INIT_KEYS: tuple = ("n_init", "random_state")
KMEANS_INIT_DEFAULTS: Dict[str, Any] = {"n_init": "auto", "random_state": 42}


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
            f"Unknown clustering method: {method!r}. Use one of {sorted(METHOD_ARGS)}."
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


def kmeans_init_args(args: Dict[str, Any], *, error) -> Dict[str, Any]:
    """
    Validate the k-means initialisation arguments in `args` and return them with
    the defaults filled in, ready to splat into `sklearn.cluster.KMeans`.

    `n_init` is `"auto"` (one k-means++ run, the scikit-learn default) or a
    positive int: the number of random initialisations, of which the best
    inertia is kept. `random_state` is an int (default 42) or `None` for a
    non-deterministic run. Passing `None` for `n_init` means the default.
    """
    n_init = args.get("n_init")
    if n_init is None:
        n_init = KMEANS_INIT_DEFAULTS["n_init"]
    is_int = isinstance(n_init, int) and not isinstance(n_init, bool)
    if not (n_init == "auto" or (is_int and n_init >= 1)):
        raise error(
            f"method_args['n_init'] must be a positive int or 'auto', got {n_init!r}."
        )
    random_state = (
        args["random_state"]
        if "random_state" in args
        else KMEANS_INIT_DEFAULTS["random_state"]
    )
    if random_state is not None and (
        not isinstance(random_state, int) or isinstance(random_state, bool)
    ):
        raise error(
            f"method_args['random_state'] must be an int or None, got {random_state!r}."
        )
    return {"n_init": n_init, "random_state": random_state}
