from typing import Iterable, Union

from .._autoflow.tracer import traced
from .._autoflow.tree import ExecutionTree
from .argument import ArgumentLink
from .nodes.base import BaseNode

Nodes = Iterable[BaseNode]
TracedNode = traced[BaseNode]
TracedNodes = Iterable[traced[BaseNode]]
FlowOutput = Union[TracedNode, TracedNodes, None]
FlowTree = ExecutionTree[BaseNode, ArgumentLink]
