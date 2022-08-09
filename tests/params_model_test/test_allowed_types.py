from src.params_model.allowed_type import _AllowedTypes
from typing import Callable


class TestAllowedTypes:

    def test_create_allowed_types(self):

        allowed_types = _AllowedTypes(init_values=(str, Callable, int))
        assert len(allowed_types) == 3
        assert str in allowed_types
        assert Callable in allowed_types
        assert int in allowed_types
