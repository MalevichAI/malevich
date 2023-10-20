from typing import Iterable, Union

from .._autoflow.tracer import traced
from .nodes.base import BaseNode

Nodes = Iterable[BaseNode]
TracedNode = traced[BaseNode]
TracedNodes = Iterable[traced[BaseNode]]
FlowOutput = Union[TracedNode, TracedNodes, None]

