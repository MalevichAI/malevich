from typing import Optional, TypeVar

from .._autoflow.tracer import traced
from ..models.nodes.base import BaseNode

T = TypeVar('T', bound=traced[BaseNode] | BaseNode)

def run(node: T, alias: Optional[str] = None) -> T:
    if alias is not None:
        if isinstance(node, traced):
            node.owner.alias = alias
        else:
            node.alias = alias
    return node
