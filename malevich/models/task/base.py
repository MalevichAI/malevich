from abc import ABC, abstractmethod

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
    def results(self):  # noqa: ANN201
        return

    @abstractmethod
    def commit_returned(self, returned: FlowOutput) -> None:
        return


    def __call__(self):  # noqa: ANN204
        self.prepare()
        self.run()
        self.stop()
        return self.results()
