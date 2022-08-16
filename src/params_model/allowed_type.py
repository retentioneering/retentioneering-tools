from collections.abc import MutableSet
from typing import Set, Tuple, Callable, Type


class _AllowedTypes(MutableSet):
    __allowed_types: Set[str] = None
    def _get_name(self, value: Type) -> str:
        return getattr(value, '__name__', None) or getattr(value, '_name', None)

    def __init__(self, init_values: Tuple[Type] = None):
        self.__allowed_types: Set[str] = set()
        names = (self._get_name(value) for value in init_values)
        names = set(filter(lambda x: x, names))
        self.__allowed_types.update(names)

    def add(self, value: Type):
        name = self._get_name(value)
        if name:
            self.__allowed_types.add(name)

    def discard(self, value: Type):
        name = self._get_name(value)
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
            value = self._get_name(item)

        if value:
            return value in self.__allowed_types

    def __getattr__(self, item: Type):
        value = self._get_name(item)
        if value:
            return value in self.__allowed_types

    def __str__(self) -> str:
        return f'({", ".join(self.__allowed_types)})'

    def __repr____(self) -> str:
        return f'({", ".join(self.__allowed_types)})'


AllowedTypes = _AllowedTypes(init_values=(str, int, float, bool, complex, Callable))
