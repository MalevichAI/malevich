from typing import Optional, TypeVar

from malevich._autoflow.tracer import traced
from malevich.models import BaseNode

T = TypeVar('T', bound=traced[BaseNode] | BaseNode)

def run(node: T, alias: Optional[str] = None) -> T:
    """Tags a node within a flow with additional metadata

    Args:
        node (T): Operation to run
        alias (Optional[str], optional): Alias of the operation. Defaults to None.
    """

    if alias is not None:
        if isinstance(node, traced):
            node.owner.alias = alias
        else:
            node.alias = alias
    return node
