

from typing import Iterable

from pydantic import ConfigDict

from ..._autoflow.tracer import traced
from ..._autoflow.tree import ExecutionTree
from .base import BaseNode


class TreeNode(BaseNode):
    tree: ExecutionTree[BaseNode]
    # FIXME: Circular import with FlowOutput
    results: Iterable[traced[BaseNode]] | traced[BaseNode] | None = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
