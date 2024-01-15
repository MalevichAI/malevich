import asyncio
import warnings
from typing import Callable, Generic, Iterable, Optional, TypeVar, Union

import pandas as pd

from ..injections import BaseInjectable
from ..types import FlowOutput
from .base import BaseTask

State = TypeVar("State")
PrepareFunc = TypeVar("PrepareFunc", bound=Callable)
RunFunc = TypeVar("RunFunc", bound=Callable)
StopFunc = TypeVar("StopFunc", bound=Callable)
ResultsFunc = TypeVar("ResultsFunc", bound=Callable)
GetInjectablesFunc = TypeVar("GetInjectablesFunc", bound=Callable)
ResultsFunc_ = Callable[
    ['InterpretedTask', FlowOutput],
    Union[Iterable[pd.DataFrame], None]
]


class InterpretedTask(Generic[State], BaseTask):
    def __init__(
        self,
        prepare: PrepareFunc | Callable[['InterpretedTask'], str],
        run: RunFunc | Callable[['InterpretedTask'], str],
        stop: StopFunc | Callable[['InterpretedTask'], None],
        results: ResultsFunc | ResultsFunc_,
        state: State,
        returned: FlowOutput = None,
        get_injectables: GetInjectablesFunc = None,
        get_operations: Optional[Callable[["InterpretedTask"], list[str]]] = None,
        configure: Optional[Callable[["InterpretedTask", str], None]] = None,
    ) -> None:
        self.__prepare = prepare
        self.__run = run
        self.__stop = stop
        self.__results = results
        self.__state = state

        self.__returned = returned

        self.__get_injectables = get_injectables
        self.__get_operations = get_operations
        self.__configure = configure

    def get_operations(self) -> list[str]:
        if self.__get_operations:
            return self.__get_operations(self)
        else:
            return []

    def configure(self, operation: str, **kwargs) -> None:
        if self.__configure:
            self.__configure(self, operation, **kwargs)
        else:
            warnings.warn(
                "The task was not interpreted with an interpreter that "
                "supports configuration. Skipping configuration for "
                "compatibility with other interpreters. "
            )


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
        callback: Optional[Callable[[Iterable[pd.DataFrame]], None]] = None,
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

    def prepare(self, *args, **kwargs) -> str:
        self.__prepare(self, *args, **kwargs)

    def run(self, *args, **kwargs) -> str:
        self.__run(self, *args, **kwargs)

    def stop(self, *args, **kwargs) -> None:
        self.__stop(self, *args, **kwargs)

    def results(self, run_id: Optional[str] = None):  # noqa: ANN201
        return self.__results(self, self.__returned, run_id=run_id)

    def commit_returned(self, returned: FlowOutput):  # noqa: ANN201
        self.__returned = returned

    def __call__(self) -> Union[Iterable[pd.DataFrame], None]:
        self.prepare()
        self.run()
        self.stop()
        results =  self.results()
        # check whether the results are iterable
        # if not, make them iterable
        try:
            iter(results)
        except TypeError:
            results = [results]
        return results


    def get_injectables(self) -> list[BaseInjectable]:
        if self.__get_injectables:
            return self.__get_injectables(self)
        else:
            warnings.warn(
                "The task was not interpreted with an interpreter that "
                "supports data injection. Returning empty list of injectables "
                "for compatibility with other interpreters. "
            )
            return []

    @property
    def state(self) -> State:
        return self.__state
