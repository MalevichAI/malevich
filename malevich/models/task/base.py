from __future__ import annotations

import asyncio
import enum
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from malevich.models import BaseInjectable, MetaEndpoint, Result, TreeNode
from malevich.types import FlowOutput

State = TypeVar("State")
CallbackParams = ParamSpec('CallbackParams')
Callback = Callable[CallbackParams, None]
StageClass = TypeVar("StageClass", bound=enum.Enum)


class BaseTask(ABC, Generic[StageClass]):
    supports_conditional_output: bool = False

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
    async def prepare(self, *args, **kwargs) -> None:
        """Prepares the task for execution"""
        pass

    @abstractmethod
    async def run(self, run_id: str | None = None, *args, **kwargs) -> None:
        """Performs a single run with a given ID"""
        pass

    @abstractmethod
    async def stop(self, *args, **kwargs) -> None:
        """Stops the task preventing it from further execution"""
        pass

    @abstractmethod
    async def results(self, *args, **kwargs) -> list[Result]:
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
    def configure(
        self,
        *operations: str,
        # Configurable parameters
        platform: str = 'base',
        platform_settings: dict[str, Any] | None = None,
        **kwargs
    )-> None:
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

