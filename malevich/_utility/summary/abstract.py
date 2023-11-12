from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from ..._autoflow.tree import ExecutionTree

State = TypeVar("State")


class AbstractSummary(ABC, Generic[State]):
    def __init__(self, tree: ExecutionTree, interpreter_state: State) -> None:
        super().__init__()

        self._tree = tree
        self._interpreter_state = interpreter_state

    @abstractmethod
    def display(self) -> None:
        pass

    @abstractmethod
    def json(self) -> dict[str, Any]:
        pass
