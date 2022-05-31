from abc import abstractmethod
from pyclbr import Function
from typing import Callable, List, Optional, Tuple, cast
import networkx
from eventstream.eventstream import Eventstream
from .factory_lib import EventsFactory

class Node():
  type: str
  events: Optional[Eventstream] = None

  def __init__(self, type: str):
      self.type = type

  @abstractmethod
  def calc_events(self, parent: Eventstream):
    pass

SOURCE_NODE = "source"
EVENTS_NODE = "events"
MERGE_NODE = "merge"

NODES_TYPES = [SOURCE_NODE, EVENTS_NODE, MERGE_NODE]

class SourceNode(Node):
  events: Eventstream

  def __init__(self, source: Eventstream):
      super().__init__(type=SOURCE_NODE)
      self.events = source

class EventsNode(Node):
  factory: EventsFactory

  def __init__(self, factory: EventsFactory):
      super().__init__(type=EVENTS_NODE)
      self.factory = factory

  def calc_events(self, parent: Eventstream):
    self.events = self.factory(parent)


class MergeNode(Node):
  def __init__(self):
      super().__init__(type=MERGE_NODE)


class PGraph():
  root: SourceNode
  __ngraph: networkx.DiGraph

  def __init__(self, source_stream: Eventstream):
    self.root = SourceNode(source=source_stream)
    self.__ngraph = networkx.DiGraph()
    self.__ngraph.add_node(self.root)

  def add_node(self, node: Node, parents: List[Node]):
    self.__valiate_already_exists([node])
    self.__validate_not_found(parents)

    if node.events is not None:
      self.__validate_schema(node.events)

    if (node.type != MERGE_NODE and len(parents) > 1):
      raise ValueError("multiple parents are only allowed for merge nodes!")

    self.__ngraph.add_node(node)

    for parent in parents:
      self.__ngraph.add_edge(parent, node)

  def combine(self, node: Node) -> Eventstream:
    self.__validate_not_found([node])

    if isinstance(node, SourceNode):
      return node.events.copy()
    
    if isinstance(node, EventsNode):
      return self.combine_events_node(node)

    if isinstance(node, MergeNode):
      return self.combine_merge_node(node)

    raise ValueError("unknown node type!")

  def combine_events_node(self, node: EventsNode):
    parent = self.get_events_node_parent(node)
    parent_events = self.combine(parent)
    events = node.factory(parent_events)
    parent_events.join_eventstream(events)
    return parent_events


  def combine_merge_node(self, node: MergeNode):
    parents = self.get_merge_node_parents(node)
    curr_eventstream: Optional[Eventstream] = None
    for parent_node in parents:
      if curr_eventstream is None:
        curr_eventstream = self.combine(parent_node)
      else:
        new_eventstream = self.combine(parent_node)
        curr_eventstream.append_eventstream(new_eventstream)

    return cast(Eventstream, curr_eventstream)

  def get_parents(self, node: Node):
    self.__validate_not_found([node])
    parents: List[Node] = []
    for parent in self.__ngraph.predecessors(node):
      parents.append(parent)
    return parents

  def get_merge_node_parents(self, node: MergeNode):
    parents = self.get_parents(node)
    if (len(parents) == 0):
      raise ValueError("orphan merge node!")

    return parents

  def get_events_node_parent(self, node: EventsNode):
    parents = self.get_parents(node)
    if len(parents) > 1:
      raise ValueError("invalid graph: events node has more than 1 parent")

    return parents[0]

  def get_graph(self):
    return self.__ngraph

  def to_json(self):
   # return networkx.tree_data(self.__ngraph)
    pass

  def __validate_schema(self, eventstream: Eventstream):
    return self.root.events.schema.is_equal(eventstream.schema)

  def __valiate_node_type(self, node: Node, expected_type: str):
    def raiseError():
      raise ValueError(f"invalid node type. Expected type is '{expected_type}'")

    if (node.type != expected_type):
      raiseError()

    if expected_type not in NODES_TYPES:
      raise ValueError("unknown expected_type")

    if expected_type == SOURCE_NODE and not isinstance(node, SourceNode):
      raiseError()

    if expected_type == EVENTS_NODE and not isinstance(node, EventsNode):
      raiseError()

    if expected_type == MERGE_NODE and not isinstance(node, MergeNode):
      raiseError()

  def __valiate_already_exists(self, nodes: List[Node]):
    for node in nodes:
      if (node in self.__ngraph.nodes):
        raise ValueError("node already exists!")

  def __validate_not_found(self, nodes: List[Node]):
    for node in nodes:
      if (node not in self.__ngraph.nodes):
        raise ValueError("node not found!")



  
      