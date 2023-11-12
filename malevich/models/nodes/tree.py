from typing import Any, Iterable, Optional

from pydantic import ConfigDict

from ..._autoflow.tracer import traced
from ..._autoflow.tree import ExecutionTree
from .base import BaseNode


class TreeNode(BaseNode):
    tree: ExecutionTree[traced[BaseNode]]
    results: Iterable[traced[BaseNode]] | traced[BaseNode] | None = None
    reverse_id: str
    name: str
    description: str = "Wonderful Flow!"
    underlying_node: Optional[Any] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __eq__(self, __value: object) -> bool:
        return self.uuid == __value.uuid
