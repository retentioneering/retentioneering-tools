from typing import List, Literal, Optional, Union

from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import (
    EnumWidget,
    IntegerWidget,
    ListOfInt,
    ListOfIntNewUsers,
    StringWidget,
)


class TestStandardWidgets:
    def test_string_widget(self) -> None:
        value = "asdasd"

        class TestStringWidgets(ParamsModel):
            a: str

            _widgets = {"a": StringWidget()}

        params = TestStringWidgets(a=value)
        widget = params.get_widgets()

        assert {"a": {"name": "a", "optional": False, "widget": "string", "default": None}} == widget
        assert value == params.a

    def test_default_string_widget(self) -> None:
        value = "asdasd"

        class TestDefaultStringWidgets(ParamsModel):
            a: str

        params = TestDefaultStringWidgets(a=value)
        widget = params.get_widgets()

        assert {"a": {"name": "a", "optional": False, "widget": "string"}} == widget
        assert value == params.a

    def test_integer_widget(self) -> None:
        value = 10**6

        class TestIntegerWidgets(ParamsModel):
            a: int

            _widgets = {"a": IntegerWidget()}

        params = TestIntegerWidgets(a=value)
        widget = params.get_widgets()

        assert {"a": {"name": "a", "optional": False, "widget": "integer", "default": None}} == widget
        assert value == params.a

    def test_default_integer_widget(self) -> None:
        value = 10**6

        class TestDefaultIntegerWidgets(ParamsModel):
            a: int

        params = TestDefaultIntegerWidgets(a=value)
        widget = params.get_widgets()

        assert {"a": {"name": "a", "optional": False, "widget": "integer"}} == widget
        assert value == params.a

    def test_enum_widget(self) -> None:
        enum_value = ["a", "b", "c"]

        class TestEnumWidgets(ParamsModel):
            a: list

            _widgets = {"a": EnumWidget(params=enum_value)}

        params = TestEnumWidgets(a=enum_value)
        widget = params.get_widgets()

        assert {
            "a": {"name": "a", "optional": False, "widget": "enum", "params": ["a", "b", "c"], "default": None}
        } == widget
        assert enum_value == params.a

    def test_default_enum_widget(self) -> None:
        enum_value = ["a", "b", "c"]

        class TestDefaultEnumWidgets(ParamsModel):
            a: list

        params = TestDefaultEnumWidgets(a=enum_value)
        widget = params.get_widgets()

        assert {"a": {"name": "a", "optional": False, "widget": "enum"}} == widget
        assert enum_value == params.a

    def test_new_user_list_widget(self) -> None:
        class TestNewUserListWidgets(ParamsModel):
            new_users_list: Union[List[int], List[str], Literal["all"]]
            _widgets = {"new_users_list": ListOfIntNewUsers()}

        params = TestNewUserListWidgets(new_users_list="all")
        widget = params.get_widgets()
        assert {
            "new_users_list": {
                "name": "new_users_list",
                "optional": False,
                "widget": "list_of_int",
                "params": {
                    "disable_value": "all",
                },
                "default": None,
            }
        } == widget

    def test_list_of_int_widget(self) -> None:
        class TestListOfIntWidgets(ParamsModel):
            lost_users_list: Optional[List[int]]
            _widgets = {"lost_users_list": ListOfInt()}

        params = TestListOfIntWidgets()
        widget = params.get_widgets()
        assert {
            "lost_users_list": {"name": "lost_users_list", "optional": True, "widget": "list_of_int", "default": None}
        } == widget
