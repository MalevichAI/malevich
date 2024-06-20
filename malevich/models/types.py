from typing import Iterable, Union

from .._autoflow.tracer import traced
from .._autoflow.tree import ExecutionTree
from .argument import ArgumentLink
from .nodes.base import BaseNode
from .python_string import PythonString  # noqa: F401 -- imported for type hinting

Nodes = Iterable[BaseNode]
TracedNode = traced[BaseNode]
TracedNodes = Iterable[traced[BaseNode]]
FlowOutput = Union[TracedNode, TracedNodes, None]
FlowTree = ExecutionTree[traced[BaseNode], ArgumentLink]
