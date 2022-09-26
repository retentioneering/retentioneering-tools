class TestCustomWidgets:
    def test_simple_custom_widget(self) -> None:
        from pydantic import Field

        from src.params_model import CustomWidgetDataType, ParamsModel

        def serialize(data):
            print(data)
            return data

        def parse(data):
            print(data)
            return data

        class TestWidgets(ParamsModel):
            a: str
            b: int
            custom_widgets: CustomWidgetDataType = Field(
                custom_widgets={"a": {"widget": "string", "serialize": serialize, "parse": parse}}
            )

        params = TestWidgets(
            a="asdasd", b=10, custom_widgets=Field({"a": {"widget": "string", "serialize": serialize, "parse": parse}})
        )
        schema = params.get_widgets()
        assert "asdasd" == params.a
        assert {"name": "a", "optional": False, "value": "asdasd", "widget": "string"} == schema["a"]
