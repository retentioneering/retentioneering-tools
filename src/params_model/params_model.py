from __future__ import annotations

from collections.abc import Iterable

from pydantic import BaseModel, validator


class ParamsModel(BaseModel):

    @validator('*')
    def validate_subiterable(cls, value):
        array_types = (Iterable, dict)
        if isinstance(value, array_types):
            try:
                if isinstance(value, dict):
                    subvalue = list(value.values())[0]
                else:
                    subvalue = next(iter(value))
                if (isinstance(subvalue, array_types) or hasattr(subvalue, "__getitem__")) \
                        and not isinstance(subvalue, str):
                    raise ValueError(f'Inner iterable or hashable not allowed!')
            except TypeError:
                pass
        return value

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
