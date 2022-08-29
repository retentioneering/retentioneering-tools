from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pytest

from src.params_model import ParamsModel


class TestParamsModel:
    def test_create_correct_model(self) -> None:
        class ExampleCorrectModel(ParamsModel):
            a: str
            b: int
            c: Callable[[int], bool]

        cor: ExampleCorrectModel = ExampleCorrectModel(a="asdasd", b=100500, c=lambda x: x > 10)
        assert cor.a == "asdasd"
        assert cor.b == 100500
        assert callable(cor.c)

    def test_nested_list(self) -> None:
        with pytest.raises(ValueError):

            class ArrayModel(ParamsModel):
                a: List[List[str]]

            ArrayModel(a=[["123123"]])

    def test_optional_none(self) -> None:
        class ArrayModel(ParamsModel):
            a: Optional[List[str]]

        model = ArrayModel(a=None)
        assert None is model.a

    def test_optional_with_value(self) -> None:
        class ArrayModel(ParamsModel):
            def __init__(self, **data: Dict[str, Any]) -> None:
                super().__init__(**data)

            a: Optional[List[str]]

        model = ArrayModel(**{"a": ["1", "2"]})  # type: ignore
        assert ["1", "2"] == model.a

    def test_nested_dict(self) -> None:
        with pytest.raises(ValueError):

            class ArrayModel(ParamsModel):
                a: Dict[str, Dict[str, str]]

            ArrayModel(a={"a": {"b": "c"}})
