import pickle
from typing import Callable

from _pickle import PicklingError
from src.params_model import ParamsModel


class Test(ParamsModel):
    a: int
    b: str
    c: Callable


def f(x):
    return x > 10


data1 = {"a": 1, "b": "asd", "c": lambda x: x > 10}

data2 = {"a": 1, "b": "asd", "c": f}

if __name__ == "__main__":

    # Cant pickle lambdas
    try:
        t = Test(**data1)
        s = pickle.dumps(t, protocol=5)
    except pickle.PicklingError as e:
        print("Dont allow lambdas in pickle")

    # Picke with function-name
    t = Test(**data2)
    ts = pickle.dumps(t, protocol=5)
    print(len(ts))
