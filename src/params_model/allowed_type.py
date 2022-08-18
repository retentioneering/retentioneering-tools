from __future__ import annotations

from collections.abc import MutableSet
from typing import Set, Tuple, Callable, Type, Optional, List, Any


class _AllowedTypes(MutableSet):
    __allowed_types: Set[str] = None

    @staticmethod
    def get_name(value: Type) -> str:
        try:
            value = getattr(value, '__str__')()
            if 'typing.Callable' in value:
                return 'typing.Callable'
            return value
        except Exception:
            return value if isinstance(value, str) else getattr(value, '__name__', None) \
                                                        or getattr(value, '_name', None)

    def __init__(self, init_values: Tuple[Any] = None):
        self.__allowed_types: Set[str] = set()
        names = (self.get_name(value) for value in init_values)
        names = set(filter(lambda x: x, names))
        self.__allowed_types.update(names)

    def add(self, value: Type):
        name = self.get_name(value)
        if name:
            self.__allowed_types.add(name)

    def discard(self, value: Type):
        name = self.get_name(value)
        if name:
            self.__allowed_types.discard(name)

    def __iter__(self):
        return iter(self.__allowed_types)

    def __len__(self):
        return len(self.__allowed_types)

    def __contains__(self, item: type):
        if isinstance(item, str) and len(item):
            value = item
        else:
            value = self.get_name(item)

        if value:
            return value in self.__allowed_types

    def __getattr__(self, item: Type):
        value = self.get_name(item)
        if value:
            return value in self.__allowed_types

    def __str__(self) -> str:
        return f'({", ".join(self.__allowed_types)})'

    def __repr____(self) -> str:
        return f'({", ".join(self.__allowed_types)})'


AllowedTypes = _AllowedTypes(
    init_values=(
        str, int, float, bool, complex, Callable, Optional[str],
        list, None, List[int], 'List[int]', List[float], 'Callable',
        'Optional[list]', 'Optional[Callable]', 'Optional[List[int]]', 'Tuple[float]', 'Tuple[float, str]',
        'Optional[Tuple[float, str]]', 'List[str]', 'list[str]', "Union[Literal['a'], Literal['b']]",
        "typing.Union[typing.Literal['a'], typing.Literal['b']]",
    )
)
