from abc import abstractmethod
from typing import Any, Callable, List, Optional, Tuple, TypedDict, cast
from xmlrpc.client import Boolean
from eventstream.eventstream import Eventstream
from dataclasses import dataclass, field

EventsFactory = Callable[[Eventstream], Eventstream]

class AvailableType:
  name: str
  @abstractmethod
  def is_valid(self, value: Any):
    return True

class String(AvailableType):
  name: str = "string"
  def is_vaid(self, value: str):
    return type(value) == "str"
   
class Enum(AvailableType):
  name: str = "enum"
  values: List[str]

  def __init__(self, values: List[str]):
    super().__init__()
    self.values = values

  def is_valid(self, value: str):
    return value in self.values
    
  


@dataclass
class DataProcessor():
  name: str
  factory: Callable[[Eventstream], Eventstream]

class ProcessorsCollection(list):
  def append(self, item: DataProcessor):
    curr_list: List[DataProcessor] = super(ProcessorsCollection, self)
      


