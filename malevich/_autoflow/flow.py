"""
AutoFlow

The package provides a context manager for providing
an access to an execution tree in function contexts.
"""

from typing import Iterable

from malevich._dev.singleton import SingletonMeta

from .tree import ExecutionTree


class Flow(metaclass=SingletonMeta):
    """Context-manager that holds the execution tree

    The class is a singleton and can be used as a context-manager
    to create a new execution tree. It holds a stack of trees to
    maintain nested flows. The stack is thread-safe.

    Example:

    .. code-block: python

        from malevich._autoflow.flow import Flow

        with Flow() as tree:
            # Work with tree
            print("Called in flow: ",  Flow.isinflow())
            print("Trees: ", len(Flow.get_stack()))
            print("In flow", Flow.get_stack(), Flow.flow_ref())
            with Flow() as inner:
                print("Trees (with inner): ", len(Flow.get_stack()))
                print("In inner", Flow.get_stack(), Flow.flow_ref())
            print("After exit", Flow.get_stack(), Flow.flow_ref())

        print("At the end", Flow.get_stack())
    """

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

    @staticmethod
    def get_stack() -> tuple[ExecutionTree]:
        """Returns immutable stack of execution trees"""
        return tuple(Flow.__flow_stack)

    @staticmethod
    def _hardset_tree(tree: ExecutionTree) -> None:
        """Injects a tree into the flow stack"""
        Flow.__flow_stack[-1] = tree
