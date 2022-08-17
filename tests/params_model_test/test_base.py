from __future__ import annotations

from typing import Any, Callable

import pytest

from src.params_model import AllowedTypes, ReteParamsModel


class TestParamsModel:
    def test_create_correct_model(self) -> None:
        class ExampleCorrectModel(ReteParamsModel):
            a: str
            b: int
            c: Callable[[int], bool]

        def f(x: int) -> bool:
            return x > 10

        correct_data: dict[str, Any] = {"a": "asdasd", "b": 100500, "c": f}
        cor: ExampleCorrectModel = ExampleCorrectModel(**correct_data)
        assert cor.a == "asdasd"
        assert cor.b == 100500
        assert callable(cor.c)

    def test_create_wrong_model(self) -> None:
        with pytest.raises(ValueError):
            # cant create incorrect params model
            class ExampleWrongModel(ReteParamsModel):  # pyright: ignore [reportUnusedClass]
                a: list[int]

    def test_custom_allowed_types(self) -> None:
        AllowedTypes.add(list)

        class ModelWithList(ReteParamsModel):
            a: list[int]

        data = {"a": [1, 2, 3]}
        model: ModelWithList = ModelWithList(**data)
        assert model.a == [1, 2, 3]
        AllowedTypes.discard(list)

    def test_custom_list_with_type(self) -> None:
        with pytest.raises(TypeError):
            AllowedTypes.add(list[int])  # pyright: ignore
