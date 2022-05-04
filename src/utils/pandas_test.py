
import unittest
import pandas as pd
from .pandas import get_merged_col


class PandasTest(unittest.TestCase):
  def test_get_merged_col(self):
    df = pd.DataFrame([
      { "a_x": 1, "b": 2 },
      { "a_x": 3, "b": 4 },
    ])

    a = get_merged_col(df=df, colname="a", suffix="_x")
    b = get_merged_col(df=df, colname="b", suffix="_x")

    self.assertEqual([a[0], a[1]], [1,3])
    self.assertEqual([b[0], b[1]], [2,4])


  


