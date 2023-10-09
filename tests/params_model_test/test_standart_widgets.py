from typing import List, Literal, Optional, Union

from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import (
    EnumWidget,
    IntegerWidget,
    ListOfIds,
    ListOfInt,
    ListOfString,
    ListOfUsers,
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

        assert {"a": {"name": "a", "optional": False, "widget": "string", "default": None}} == widget
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

        assert {"a": {"name": "a", "optional": False, "widget": "integer", "default": None}} == widget
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

        assert {"a": {"name": "a", "optional": False, "widget": "enum", "default": None}} == widget
        assert enum_value == params.a

    def test_list_of_users_widget(self) -> None:
        class TestNewUserListWidgets(ParamsModel):
            new_users_list1: Union[List[int], List[str], Literal["all"]]
            _widgets = {"new_users_list1": ListOfUsers()}

        params = TestNewUserListWidgets(new_users_list1="all")
        widget = params.get_widgets()
        assert {
            "new_users_list1": {
                "name": "new_users_list1",
                "optional": False,
                "widget": "list_of_ids",
                "params": {
                    "disable_value": "all",
                },
                "default": None,
            }
        } == widget

    def test_list_of_ids_widget(self) -> None:
        class TestListOfIdsWidgets(ParamsModel):
            lost_users_list: Optional[Union[List[str], List[int]]]
            _widgets = {"lost_users_list": ListOfIds()}

        params = TestListOfIdsWidgets(lost_users_list=[])
        widget = params.get_widgets()
        assert {
            "lost_users_list": {"name": "lost_users_list", "optional": True, "widget": "list_of_ids", "default": None}
        } == widget

    def test_list_of_int_widget(self) -> None:
        class TestListOfIntWidgets(ParamsModel):
            some_list: Optional[List[int]]
            _widgets = {"some_list": ListOfInt()}

        params = TestListOfIntWidgets(some_list=[123])
        widget = params.get_widgets()
        assert {
            "some_list": {"name": "some_list", "optional": True, "widget": "list_of_int", "default": None}
        } == widget

    def test_list_of_string_widget(self) -> None:
        class TestListOfStringWidgets(ParamsModel):
            some_list: Optional[List[str]]
            _widgets = {"some_list": ListOfString()}

        params = TestListOfStringWidgets(some_list=["123"])
        widget = params.get_widgets()
        assert {
            "some_list": {"name": "some_list", "optional": True, "widget": "list_of_string", "default": None}
        } == widget
