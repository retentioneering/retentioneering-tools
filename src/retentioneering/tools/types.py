from collections.abc import Iterable
from typing import Literal

T_TransitionMatrixValues = Literal[
    "count",
    "unique_paths",
    "share_of_total",
    "avg_per_path",
    "proba_in",
    "proba_out",
    "time_median",
    "time_q95",
]

# tuple/list, not Sequence[str]: a bare str also satisfies Sequence[str].
T_Diff = (
    tuple[str, str, str]
    | list[str]
    | tuple[Iterable[str | int], Iterable[str | int]]
    | list[Iterable[str | int]]
    | None
)
