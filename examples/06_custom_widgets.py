from src.params_model import CUSTOM_WIDGET, ParamsModel


def serialize(data):
    print(data)
    return data


def parse(data):
    print(data)
    return data


class TestWidgets(ParamsModel):
    a: str

    custom_widgets = {"a": {"widget": "string", "serialize": serialize, "parse": parse}}
