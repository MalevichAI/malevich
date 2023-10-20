from typing import Callable, Generic, Iterable, TypeVar, Union

import pandas as pd

from ..types import FlowOutput
from .base import BaseTask

State = TypeVar("State")

class InterpretedTask(Generic[State], BaseTask):
    def __init__(
       self,
       prepare: Callable[['InterpretedTask'], State],
       run: Callable[['InterpretedTask'], None],
       stop: Callable[['InterpretedTask'], None],
       results: Callable[['InterpretedTask', FlowOutput], Union[Iterable[pd.DataFrame], pd.DataFrame, None]],  # noqa: E501
       state: State,
       returned: FlowOutput = None,
   ) -> None:
         self.__prepare = prepare
         self.__run = run
         self.__stop = stop
         self.__results = results
         self.__state = state
         self.__returned = returned

    def prepare(self) -> None:
        self.__prepare(self)

    def run(self) -> None:
        self.__run(self)

    def stop(self) -> None:
        self.__stop(self)

    def results(self):  # noqa: ANN201
        return self.__results(self, self.__returned)

    def commit_returned(self, returned: FlowOutput):  # noqa: ANN201
        self.__returned = returned

    def __call__(self) -> Union[Iterable[pd.DataFrame], pd.DataFrame, None]:
        self.prepare()
        self.run()
        self.stop()
        return self.results()

    @property
    def state(self) -> State:
        return self.__state
