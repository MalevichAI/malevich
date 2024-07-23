from .base import BaseNode
from .operation import OperationNode

class JointNode(BaseNode):
    nodes: list[tuple[BaseNode, OperationNode, bool | None]] = []
