from typing import Callable

import pytest

from src.params_model import ReteParamsModel


class TestParamsModel:
    def test_create_correct_mode(self):
        class ExampleCorrectModel(ReteParamsModel):
            a: str
            b: int
            c: Callable

        correct_data = {'a': 'asdasd', 'b': 100500, 'c': lambda x: x > 10}
        cor: ExampleCorrectModel = ExampleCorrectModel(**correct_data)
        assert cor.a == 'asdasd'
        assert cor.b == 100500
        assert callable(cor.c)

    def test_create_wrong_model(self):

        with pytest.raises(ValueError):
            # cant create incorrect params model
            class ExampleWrongModel(ReteParamsModel):
                a: list
