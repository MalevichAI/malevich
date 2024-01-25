from abc import ABC, abstractmethod
from typing import Optional

from ..injections import BaseInjectable
from ..results.base import BaseResult
from ..types import FlowOutput


class BaseTask(ABC):
    @abstractmethod
    def prepare(self, *args, **kwargs) -> None:
        return

    @abstractmethod
    def run(self, run_id: Optional[str] = None, *args, **kwargs) -> None:
        return

    @abstractmethod
    def stop(self, *args, **kwargs) -> None:
        return

    @abstractmethod
    def results(self, *args, **kwargs) -> list[BaseResult]:
        return

    @abstractmethod
    def commit_returned(self, returned: FlowOutput, *args, **kwargs) -> None:
        return

    @abstractmethod
    def get_injectables(self, *args, **kwargs) -> list[BaseInjectable]:
        return

    def get_operations(self, *args, **kwargs) -> list[str]:
        return []

    def configure(self, operation: str, **kwargs) -> None:
        return

    def __call__(self) -> list[BaseResult]:
        self.prepare()
        self.run()
        self.stop()
        return self.results()
