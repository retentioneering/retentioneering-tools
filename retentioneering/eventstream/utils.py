from typing import Generator


def get_eventstream_index() -> Generator:
    idx = 1
    while True:
        yield idx
        idx += 1


eventstream_index = get_eventstream_index()
