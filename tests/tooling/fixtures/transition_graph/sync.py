import json

from pytest import fixture


@fixture
def sync_payload():
    return json.loads("./sync_dump_simple_shop.json")
