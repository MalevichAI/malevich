from abc import ABC, abstractmethod
from malevich._autoflow.tree import ExecutionTree
from pydantic import BaseModel

class Interpreter:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def interpret(self, tree: ExecutionTree) -> None:
        pass

    @abstractmethod
    def get_model(self) -> BaseModel:
        pass
