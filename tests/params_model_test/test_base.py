from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pytest

from retentioneering.params_model import ParamsModel


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
        class ArrayModel1(ParamsModel):
            a: Optional[List[str]]

        model = ArrayModel1(a=None)
        assert None is model.a

    def test_optional_with_value(self) -> None:
        class ArrayModel2(ParamsModel):
            def __init__(self, **data: Dict[str, Any]) -> None:
                super().__init__(**data)

            a: Optional[List[str]]

        model = ArrayModel2(**{"a": ["1", "2"]})  # type: ignore
        assert ["1", "2"] == model.a

    def test_nested_dict(self) -> None:
        with pytest.raises(ValueError):

            class ArrayModel3(ParamsModel):
                a: Dict[str, Dict[str, str]]

            ArrayModel3(a={"a": {"b": "c"}})

    def test_widget(self) -> None:
        class ExampleModel(ParamsModel):
            a: str

        model = ExampleModel(a="asd")
        assert "asd" == model.a

    def test_get_values(self) -> None:
        import inspect

        @dataclass
        class TestWidget:
            widget: str = "string"

            @classmethod
            def from_dict(cls, **kwargs) -> "TestWidget":
                return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

            @classmethod
            def _serialize(cls, value) -> str:
                return str(value) * 3

        class ExampleModelExport(ParamsModel):
            a: str

            _widgets = {"a": TestWidget()}

        model = ExampleModelExport(a="asd")
        data = model.dict()
        assert {"a": "asdasdasd"} == data
