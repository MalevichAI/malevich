

from typing import Any, Iterable, Optional, List

from pydantic import ConfigDict

from ..._autoflow.tracer import traced
from ..._autoflow.tree import ExecutionTree
from .base import BaseNode


class TreeNode(BaseNode):
    tree: ExecutionTree[traced[BaseNode]]
    # FIXME: Circular import with FlowOutput
    results: Iterable[traced[BaseNode]] | traced[BaseNode] | None = None
    reverse_id: str
    name: str
    # parent_child_map: dict[BaseNode, Optional[BaseNode]] = {}
    # input_nodes: list[traced[BaseNode]]
    # output_nodes: list[traced[BaseNode]]
    underlying_node: Optional[Any] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )


# class TreeInputNode(BaseNode):
#     tree: TreeNode
#     parent_node: BaseNode
#     input_index: int


# class TreeOutputNode(BaseNode):
#     tree: TreeNode
#     parent_node: BaseNode
#     output_index: int
