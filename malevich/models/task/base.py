from __future__ import annotations

import asyncio
import enum
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from ..endpoint import MetaEndpoint

# from ...interpreter.abstract import Interpreter
from ..injections import BaseInjectable
from ..nodes.tree import TreeNode
from ..results import Result
from ..types import FlowOutput

State = TypeVar("State")
CallbackParams = ParamSpec('CallbackParams')
Callback = Callable[CallbackParams, None]
StageClass = TypeVar("StageClass", bound=enum.Enum)


class BaseTask(ABC, Generic[StageClass]):
    @property
    @abstractmethod
    def tree(self) -> TreeNode:
        """Returns the execution tree, encapsulated into TreeNode class"""
        pass

    @abstractmethod
    def get_stage(self) -> StageClass:
        pass

    def get_stage_class(self) -> type[StageClass]:
        return StageClass

    # Main Task Operations
    # ====================
    @abstractmethod
    def interpret(self, interpreter: Any = None) -> None:  # noqa: ANN401
        """Attaches the task to particular platform"""
        pass

    @abstractmethod
    def prepare(self, *args, **kwargs) -> None:
        """Prepares the task for execution"""
        pass

    def async_prepare(
        self,
        *args,
        callback: Callback = None,
        cb_args: tuple = (),
        cb_kwargs: dict = {},
        **kwargs
    ) -> None:
        """Asynchronously prepares the task for execution
        and calls the callback when done"""
        (   # Executes sync code in a separate thread
            asyncio.get_event_loop()
            .run_in_executor(None, self.prepare, *args, **kwargs)
            .add_done_callback(lambda _: callback(*cb_args, **cb_kwargs))
        )
        # self.prepare(self, *args, **kwargs)
        # if callback:
        #     callback(*cb_args, **cb_kwargs)

    @abstractmethod
    def run(self, run_id: str | None = None, *args, **kwargs) -> None:
        """Performs a single run with a given ID"""
        pass

    def async_run(
        self,
        *args,
        callback: Callback = None,
        cb_args: tuple = (),
        cb_kwargs: dict = {},
        run_id: str | None = None,
        **kwargs
    ) -> None:
        """Asynchronously performs a single run with a given ID"""
        (
            asyncio.get_event_loop()
            .run_in_executor(None, self.run, run_id, *args, **kwargs)
            .add_done_callback(lambda _: callback(*cb_args, **cb_kwargs))
        )
        # await asyncio.sleep(0)
        # self.run(self, *args, run_id=run_id, **kwargs)
        # if callback:
        #     callback(*cb_args, **cb_kwargs)

    @abstractmethod
    def stop(self, *args, **kwargs) -> None:
        """Stops the task preventing it from further execution"""
        pass

    def async_stop(
        self,
        *args,
        callback: Callback = None,
        cb_args: tuple = (),
        cb_kwargs: dict = {},
        **kwargs
    ) -> None:
        """Asynchronously stops the task preventing it from further execution"""
        (
            asyncio.get_event_loop()
            .run_in_executor(None, self.stop, *args, **kwargs)
            .add_done_callback(lambda _: callback(*cb_args, **cb_kwargs))
        )
        # await asyncio.sleep(0)
        # self.stop(self, *args, **kwargs)
        # if callback:
        #     callback(*cb_args, **cb_kwargs)

    @abstractmethod
    def results(self, *args, **kwargs) -> list[Result]:
        """Retrieves results of the run"""
        pass

    # Auxillary methods (used by flows)
    # =================================
    @abstractmethod
    def commit_returned(self, returned: FlowOutput) -> None:
        """Reports what part of the execution tree should be used to retrieve results"""
        pass

    # Saving / Loading
    # ================
    @abstractmethod
    def dump(self) -> bytes:
        """Serializes the task into bytes"""
        pass

    @staticmethod
    @abstractmethod
    def load(self, object_bytes: bytes) -> BaseTask:
        """Deserializes the task from bytes"""
        pass

    # Configuration methods (for runs)
    # ================================
    @abstractmethod
    def get_injectables(self) -> list[BaseInjectable]:
        """Returns a list of possible tweaks for the run"""
        pass

    @abstractmethod
    def get_operations(self) -> list[str]:
        """Returns a operations within the task"""
        pass

    @abstractmethod
    def configure(self, operation: str, **kwargs) -> None:
        """Configures a given operation within the task"""
        pass

    # Interpretation
    # ==============
    @abstractmethod
    def get_interpreted_task(self) -> BaseTask:
        """Returns the task interpreted for a particular platform"""
        pass


    def publish(self, *args, **kwargs) -> MetaEndpoint:
        raise NotImplementedError(
            f"Publishing is not available for {self.__class__.__name__}"
        )

