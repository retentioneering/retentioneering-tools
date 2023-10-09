from __future__ import annotations

import pandas as pd
import pytest

from retentioneering.utils.flatten_list import flatten


class TestFlattenList:
    def test_simple(self) -> None:
        assert flatten([1, 2, 3]) == [1, 2, 3]
        assert flatten([1, 2, 3, 4]) == [1, 2, 3, 4]
        assert flatten([1, 2, 3, 4, 5]) == [1, 2, 3, 4, 5]
        assert flatten([1, 2, 3, 4, 5, 6]) == [1, 2, 3, 4, 5, 6]
        assert flatten([1, 2, 3, 4, 5, 6, 7]) == [1, 2, 3, 4, 5, 6, 7]

    def test_nested_list(self) -> None:
        assert flatten([[1, 2, 3], [4, 5, 6]]) == [1, 2, 3, 4, 5, 6]
        assert flatten([[1, 2, 3], [4, 5, 6], [7, 8, 9]]) == [1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_unexpected_dictionary(self) -> None:
        assert flatten({"a": 1, "b": 2}) == ["a", "b"]

    def test_mixed_types(self) -> None:
        # set doesn't save order'
        assert (flatten({"a", ("b", "c")}) == ["a", "b", "c"]) or (flatten({"a", ("b", "c")}) == ["b", "c", "a"])
        assert flatten((pd.DataFrame(columns=["a", "b"]).columns, pd.Series(["c", "d"]))) == ["a", "b", "c", "d"]
