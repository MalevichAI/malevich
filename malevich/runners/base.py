from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from ..models.flow_function import FlowFunction
from ..models.injections import BaseInjectable
from ..models.task.promised import PromisedTask

Injection = TypeVar("Injection", bound=BaseInjectable)


class BaseRunner(ABC, Generic[Injection]):
    def __init__(self, task: FlowFunction[...,  PromisedTask]) -> None:
        super().__init__()
        self._base_task_f = task

    @abstractmethod
    def run(**injections: Injection) -> None:
        pass

    @abstractmethod
    def stop() -> None:
        pass
