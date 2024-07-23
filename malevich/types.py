from typing import Iterable, Union

from malevich._autoflow.tracer import traced
from malevich._autoflow.tree import ExecutionTree
from malevich.models import ArgumentLink, BaseNode

Nodes = Iterable[BaseNode]
TracedNode = traced[BaseNode]
TracedNodes = Iterable[traced[BaseNode]]
FlowOutput = Union[TracedNode, TracedNodes, None]
FlowTree = ExecutionTree[traced[BaseNode], ArgumentLink]
