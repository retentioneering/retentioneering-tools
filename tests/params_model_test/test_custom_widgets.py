from typing import Tuple


class TestCustomWidgets:
    def test_simple_custom_widget(self) -> None:

        from src.params_model import CustomWidgetDataType, ParamsModel
        from src.params_model.params_model import CustomWidgetProperties

        def serialize(data: Tuple[int, str]) -> str:
            return ",".join([str(x) for x in data])

        def parse(data: str) -> Tuple:
            return tuple(data.split())

        class TestWidgets(ParamsModel):
            a: Tuple[int, str]
            b: int

            class AdditionalParams:
                custom_widgets: CustomWidgetDataType = CustomWidgetDataType(
                    a=CustomWidgetProperties(widget="string", serialize=serialize, parse=parse)
                )

        params = TestWidgets(a=(1, "asd"), b=10)

        schema = params.get_widgets()
        assert (1, "asd") == params.a
        assert {"name": "a", "optional": False, "value": "1,asd", "widget": "string"} == schema["a"]
