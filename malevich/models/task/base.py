from abc import ABC, abstractmethod
from typing import Any

from malevich.models.types import FlowOutput


class BaseTask(ABC):
    @abstractmethod
    def prepare(self) -> None:
        return

    @abstractmethod
    def run(self) -> None:
        return

    @abstractmethod
    def stop(self) -> None:
        return

    @abstractmethod
    def results(self) -> None:
        return

    @abstractmethod
    def commit_returned(self, returned: FlowOutput) -> None:
        return


    def __call__(self) -> Any:
        self.prepare()
        self.run()
        self.stop()
        return self.results()
