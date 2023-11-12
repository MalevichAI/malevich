from typing import Iterable

from .._utility.singleton import SingletonMeta
from .tree import ExecutionTree


class Flow(metaclass=SingletonMeta):
    """Orchestrate the execution of the pipeline"""

    # Internal representation of the execution tree
    __flow_stack = []

    def isinflow() -> bool:
        """Check if the current execution is inside a flow"""
        return len(Flow.__flow_stack) > 0

    def flow_ref() -> ExecutionTree:
        """Returns the reference to the current flow in the context"""
        return None if not Flow.isinflow() else Flow.__flow_stack[-1]

    def __enter__(self) -> ExecutionTree:
        Flow.__flow_stack.append(ExecutionTree())
        return Flow.flow_ref()

    def __exit__(self, *args: Iterable) -> None:
        Flow.__flow_stack.pop()

    @property
    def stack(self) -> list[ExecutionTree]:
        return Flow.__flow_stack
