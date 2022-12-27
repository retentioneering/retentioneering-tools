import pytest

from src import datasets


@pytest.fixture
def stream_simple_shop():
    test_stream = datasets.load_simple_shop()
    return test_stream
