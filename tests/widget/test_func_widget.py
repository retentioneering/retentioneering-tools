import pandas as pd

from retentioneering.widget.widgets import ReteFunction

func_with_import = """
def func():
  import pandas as pd
  df = pd.DataFrame([{ "a": "d", "b": "s"  }, { "a": "dd", "b": "ss"  }])
  return df
"""


class TestFuncWidget:
    def test_serialize_and_parse(self):
        rete_func_widget = ReteFunction()

        def myfunc(a, b):
            return a + b

        serialized = rete_func_widget._serialize(myfunc)
        restored_func = rete_func_widget._parse(serialized)
        assert restored_func(1, 2) == myfunc(1, 2)
        assert restored_func(0, 1) == 1

    def test_func_with_import(self):
        rete_func_widget = ReteFunction()
        parsedfunc = rete_func_widget._parse(func_with_import)
        result = parsedfunc()

        expected_result = pd.DataFrame([{"a": "d", "b": "s"}, {"a": "dd", "b": "ss"}])
        assert result[expected_result.columns].compare(expected_result).shape == (0, 0)
