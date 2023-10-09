from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Extra, create_model_from_typeddict
from typing_extensions import (  # required for pydantic and python < 3.9.2
    NotRequired,
    Required,
    TypedDict,
)


class NodeData(TypedDict, total=False):
    name: Required[str]
    pk: Required[str]
    description: NotRequired[str]
    processor: NotRequired[Dict]


class NodeLink(TypedDict):
    source: str
    target: str


class Payload(TypedDict):
    directed: bool
    nodes: list[NodeData]
    links: list[NodeLink]


class CombineHandlerPayload(TypedDict):
    node_pk: str


class FieldErrorDesc(TypedDict):
    field: str
    msg: str


class CreateNodeErrorDesc(TypedDict):
    type: Literal["node_error"]
    node_pk: str
    msg: Optional[str]
    fields_errors: List[FieldErrorDesc]


class PreprocessingGraphConfig(BaseModel):
    directed: bool
    nodes: List[NodeData]
    links: List[NodeLink]

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow
