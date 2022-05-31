from typing import Callable, List, Optional, Tuple, cast
from eventstream.eventstream import Eventstream
from dataclasses import dataclass, field

EventsFactory = Callable[[Eventstream], Eventstream]

@dataclass
class DataProcessor():
  name: str
  factory: Callable[[Eventstream], Eventstream]

class ProcessorsCollection(list):
  def append(self, item: DataProcessor):
    curr_list: List[DataProcessor] = super(ProcessorsCollection, self)
      


