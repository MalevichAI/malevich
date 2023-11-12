import asyncio
from typing import Callable, Generic, Iterable, Optional, TypeVar, Union

import pandas as pd

from ..types import FlowOutput
from .base import BaseTask

State = TypeVar("State")
PrepareFunc = TypeVar("PrepareFunc", bound=Callable)
RunFunc = TypeVar("RunFunc", bound=Callable)
StopFunc = TypeVar("StopFunc", bound=Callable)
ResultsFunc = TypeVar("ResultsFunc", bound=Callable)
ResultsFunc_ = Callable[
    ['InterpretedTask', FlowOutput],
    Union[Iterable[pd.DataFrame], pd.DataFrame, None]
]


class InterpretedTask(Generic[State], BaseTask):
    def __init__(
        self,
        prepare: PrepareFunc | Callable[['InterpretedTask'], None],
        run: RunFunc | Callable[['InterpretedTask'], None],
        stop: StopFunc | Callable[['InterpretedTask'], None],
        results: ResultsFunc | ResultsFunc_,
        state: State,
        returned: FlowOutput = None,
    ) -> None:
        self.__prepare = prepare
        self.__run = run
        self.__stop = stop
        self.__results = results
        self.__state = state
        self.__returned = returned

    async def async_prepare(
        self,
        callback: Optional[Callable[[], None]] = None,
        *args,
        **kwargs
    ) -> None:
        """Prepare the task for execution.

        Args:
            callback (function, optional): Callback to be run after
                A callback to be run after the task is prepared. A
                function that takes no arguments and returns nothing.
            *args (Any, optional): Positional arguments to be passed to task
                Positional arguments to be passed to the prepare function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
            **kwargs (Any, optional): Keyword arguments to be passed to task
                Keyword arguments to be passed to the prepare function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
        """
        # Run prepare asynchronously and then
        # callback in provided
        await asyncio.sleep(0)
        self.__prepare(self, *args, **kwargs)
        if callback:
            callback()

    async def async_run(
        self,
        callback: Optional[Callable[[], None]] = None,
        *args,
        **kwargs
    ) -> None:
        """Run the task.

        Args:
            callback (function, optional): Callback to be run after
                A callback to be run after the task is prepared. A
                function that takes no arguments and returns nothing.
            *args (Any, optional): Positional arguments to be passed to task
                Positional arguments to be passed to the run function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
            **kwargs (Any, optional): Keyword arguments to be passed to task
                Keyword arguments to be passed to the run function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
        """
        # Run run asynchronously and then
        # callback in provided
        await asyncio.sleep(0)
        self.__run(self, *args, **kwargs)
        if callback:
            callback()

    async def async_stop(
        self,
        callback: Optional[Callable[[], None]] = None,
        *args,
        **kwargs
    ) -> None:
        """Stop the task.

        Args:
            callback (function, optional): Callback to be run after
                A callback to be run after the task is prepared. A
                function that takes no arguments and returns nothing.
            *args (Any, optional): Positional arguments to be passed to task
                Positional arguments to be passed to the stop function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
            **kwargs (Any, optional): Keyword arguments to be passed to task
                Keyword arguments to be passed to the stop function
                of the produced task. You should check the documentation
                of an interpreter that you are using to see what arguments
                the task expects.
        """
        # Run stop asynchronously and then
        # callback in provided
        await asyncio.sleep(0)
        self.__stop(self, *args, **kwargs)
        if callback:
            callback()

    async def async_results(
        self,
        callback: Optional[Callable[[pd.DataFrame | Iterable[pd.DataFrame]], None]] = None,  # noqa: E501
    ) -> None:
        """Get results of the task.

        Args:
            callback (function, optional): Callback to be run after
                A callback to be run after the task is prepared. A
                function that takes no arguments and returns nothing.

                The argument of the callback is a pandas DataFrame
                or an iterable of pandas DataFrames.
        """
        # Run results asynchronously and then
        # callback in provided
        await asyncio.sleep(0)
        results = self.__results(self, self.__returned)
        if callback:
            callback(results)

    def prepare(self, *args, **kwargs) -> None:
        self.__prepare(self, *args, **kwargs)

    def run(self, *args, **kwargs) -> None:
        self.__run(self, *args, **kwargs)

    def stop(self, *args, **kwargs) -> None:
        self.__stop(self, *args, **kwargs)

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
