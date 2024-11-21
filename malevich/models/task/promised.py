
import pickle

# from .interpreted import InterpretedTask
from enum import Enum
from typing import Any, Type

from malevich_space.schema import ComponentSchema

from malevich.types import FlowOutput

from ..endpoint import MetaEndpoint
from ..nodes.tree import TreeNode
from ..results import Result
from .base import BaseInjectable, BaseTask


class PromisedStage(Enum):
    NOT_INTERPRETED = "NOT_INTERPRETED"

# Proxy Pattern
class PromisedTask(BaseTask[PromisedStage]):
    """An intermediate representation of a task that can be interpreted

    Interpretation of a task means attaching it to a particular platform
    and preparing it for execution. This is done by calling :meth:`interpret`
    method.

    Once the task is interpreted, the following methods become available:

    - :meth:`prepare`
    - :meth:`run`
    - :meth:`stop`
    - :meth:`results`
    """
    def __init__(
        self,
        results: FlowOutput,
        tree: TreeNode,
        component: ComponentSchema
    ) -> None:
        self.__results = results
        self.__tree = tree
        self.__task = None
        self.__conf_memory = []
        self._component = component

    @property
    def tree(self) -> TreeNode:
        return self.__tree

    def _attach_task(self, _task: BaseTask) -> None:
        self.__task = _task

    def get_stage(self) -> Any:  # noqa: ANN401
        """Retrieves the current stage of the task.

        If the task has not been interpreted, the stage is `NOT_INTERPRETED`.
        Otherwise, the stage is defined by the underlying interpreted task.
        """
        if self.__task:
            return self.__task.get_stage()
        else:
            return PromisedStage.NOT_INTERPRETED

    def get_stage_class(self) -> Type:
        """Returns the enum class of the stage of the task

        If the task has not been interpreted, the class is `PromisedStage`.
        Otherwise, the class is defined by the underlying interpreted task.
        """
        if self.__task:
            return self.__task.get_stage_class()
        else:
            return PromisedStage

    def interpret(
        self,
        interpreter: 'malevich._interpreter.abstrct.Interpreter' = None
    ) -> None:
        """Interprets the task with a particular interpreter

        Attaching a task to a particular platform and preparing it for execution.
        This is done by calling :meth:`interpret`. Once the task is interpreted,
        it can be prepared, run, stopped and its results can be fetched.

        The task might be re-interpreted with a different interpreter. In this
        case, the previous interpreter is discarded and the new one is used.

        Args:
            interpreter (:class:`malevich.interpreter.Interpreter`, optional):
                Interpreter to use. :class:`malevich.interprete.SpaceInterpreter`
                is used when not specified. Defaults to None.
        """
        from malevich.interpreter import SpaceInterpreter
        __interpreter = interpreter or SpaceInterpreter()
        try:
            task = __interpreter.interpret(self.__tree, self._component)
            if self.__results:
                task.commit_returned(self.__results)
            self.__task = task
            # Apply the configuration that was stored in memory
            for operation, kwargs in self.__conf_memory:
                self.__task.configure(operation, **kwargs)
            del self.__conf_memory
        except Exception as e:
           if not interpreter:
               raise Exception(
                    "Attempt to interpret task with default interpreter failed. "
                    "Try to specify interpreter explicitly"
                ) from e
           else:
                raise e

    async def prepare(self, *args, **kwargs) -> None:
        """Prepares necessary data for the task to be executed (run)

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return await self.__task.prepare(*args, **kwargs)

    async def run(
        self,
        run_id: str | None = None,
        override: dict[str, Any] | None = None,
        *args,
        **kwargs
    ) -> None:
        """Runs the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if override is None:
            override = dict()

        if not self.__task:
            raise Exception(
                "Unable to run task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return await self.__task.run(
            *args,
            run_id=run_id,
            override=override,
            **kwargs
        )

    async def stop(self, *args, **kwargs) -> None:
        """Stops the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return await self.__task.stop(*args, **kwargs)

    async def results(self, *args, **kwargs) -> list[Result]:
        """Retrieve results of the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to get results of the task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return await self.__task.results(*args, **kwargs)

    def commit_returned(self, returned: FlowOutput) -> None:
        """Saves objects to determine the results of the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to commit returned results to the task, "
                "that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.commit_returned(returned)

    def dump(self) -> bytes:
        """Serialize the task to bytes, which can be saved and used to load it again"""
        if self.__task:
            task_bytes_ = self.__task.dump()
        else:
            task_bytes_ = b""
        tree_bytes_ = pickle.dumps(self.__tree.model_dump())
        results_bytes_ = pickle.dumps(self.__results)
        component_bytes_ = pickle.dumps(self._component)
        return pickle.dumps(
            (task_bytes_, tree_bytes_, results_bytes_, component_bytes_)
        )

    @staticmethod
    def load(object_bytes: bytes) -> 'PromisedTask':
        """Static method. Deserialize bytes into task object"""
        task_bytes_, tree_bytes_, results_bytes_, component_bytes_ = pickle.loads(object_bytes)  # noqa: E501
        task = PromisedTask(
            results=pickle.loads(results_bytes_),
            tree=TreeNode(**pickle.loads(tree_bytes_)),
            component=pickle.loads(component_bytes_)
        )
        if len(task_bytes_) > 0:
            task._attach_task(pickle.loads(task_bytes_))

        return task


    def get_injectables(self) -> list[BaseInjectable]:
        """Returns a list of possible injections into run

        See the documentation of the corresponding
        interpreter used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to get injectables. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        return self.__task.get_injectables()

    def get_operations(self) -> list[str]:
        """Returns a list of operations

        The term operations is defined by the interpreter used.
        See the documentation of the corresponding
        interpreter used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to get operations. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        return self.__task.get_operations()

    def configure(self, *operations: str, **kwargs) -> None:
        """Configures the task for a particular operation

        What is configurable and how it is configurable is defined by the
        interpreter used. See the documentation of the corresponding
        interpreter used before calling this method.

        Args:
            operation (str): Operation to configure (one from :meth:`get_operations`)
            **kwargs (Any): Arguments to configure the operation
        """
        if not self.__task:
            for op in operations:
            # Remember the configuration to apply it later
                self.__conf_memory.append((op, kwargs))
            return None
        # Otherwise proxy directly
        return self.__task.configure(*operations, **kwargs)

    def get_interpreted_task(self) -> BaseTask:
        """Retrieves the interpreted task that is produced when calling :meth:`interpret`

        Returns:
            :class:`BaseTask`: The interpreted task
        """  # noqa: E501
        if not self.__task:
            raise Exception(
                "Unable to get interpreted task. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        return self.__task

    def publish(self, *args, **kwargs) -> MetaEndpoint:
        """Creates a HTTP endpoint for the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.

        Returns:
            :class:`malevich.models.endpoint.MetaEndpoint`: An endpoint object
        """
        if not self.__task:
            raise Exception(
                "Cannot publish uninterpreted task. "
                "Use `.interpret()` first."
            )
        return self.__task.publish(*args, **kwargs)
