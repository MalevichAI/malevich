from typing import Iterable

from malevich._autoflow.singleton import SingletonMeta
from malevich._autoflow.tree import ExecutionTree


class Flow:
    """Orchestrate the execution of the pipeline"""

    # Internal representation of the execution tree
    __flow = None

    def isinflow() -> bool:
        """Check if the current execution is inside a flow"""
        return Flow.__flow is not None

    def flow_ref() -> ExecutionTree:
        """Returns the reference to the current flow in the context"""
        return Flow.__flow


    def __enter__(self) -> ExecutionTree:
        assert Flow.__flow is None, "Flow already exists"
        Flow.__flow = ExecutionTree()
        return Flow.__flow


    def __exit__(self, *args: Iterable) -> None:
        Flow.__flow = None


class Registry(metaclass=SingletonMeta):
    _registry = {}

    def register(self, key: str, value: object) -> None:
        self._registry[key] = value

    def get(self, key: str) -> object:
        return self._registry[key]

    def __getitem__(self, key: str) -> object:
        return self._registry[key]
