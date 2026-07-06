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

T_Diff = tuple[str, str, str] | tuple[list[str | int], list[str | int]] | None
