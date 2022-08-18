from __future__ import annotations

from typing import Callable

import pytest

from src.params_model import AllowedTypes, ParamsModel


class TestParamsModel:
    def test_create_correct_model(self) -> None:
        class ExampleCorrectModel(ParamsModel):
            a: str
            b: int
            c: Callable

        correct_data = {'a': 'asdasd', 'b': 100500, 'c': lambda x: x > 10}
        cor: ExampleCorrectModel = ExampleCorrectModel(**correct_data)
        assert cor.a == 'asdasd'
        assert cor.b == 100500
        assert callable(cor.c)

    def test_create_wrong_model(self) -> None:
        with pytest.raises(ValueError):
            # cant create incorrect params model
            class ExampleWrongModel(ParamsModel):
                a: frozenset

    def test_custom_allowed_types(self) -> None:
        AllowedTypes.add(list)

        class ModelWithList(ParamsModel):
            a: list

        data = {'a': [1, 2, 3]}
        model: ModelWithList = ModelWithList(**data)
        assert model.a == [1, 2, 3]
        AllowedTypes.discard(list)

    def test_custom_list_with_type(self) -> None:
        with pytest.raises(TypeError):
            AllowedTypes.add(list[int])

