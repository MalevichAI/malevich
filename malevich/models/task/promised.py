
import os
import pickle
from typing import Optional

from ...interpreter.abstract import Interpreter
from ...interpreter.space import SpaceInterpreter
from ...manifest import ManifestManager
from ...models.nodes.tree import TreeNode
from ...models.task.base import BaseTask
from ...models.types import FlowOutput
from ..injections import BaseInjectable
from ..results import Result
from .interpreted import InterpretedTask


class PromisedTask(BaseTask):
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
    ) -> None:
        self.__results = results
        self.__tree = tree
        self.__task = None

    @property
    def tree(self) -> TreeNode:
        return self.__tree

    def interpret(self, interpreter: Interpreter = None) -> None:
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
        __interpreter = interpreter or SpaceInterpreter()
        try:
            task = __interpreter.interpret(self.__tree)
            task.commit_returned(self.__results)
            self.__task = task
        except Exception as e:
           if not interpreter:
               raise Exception(
                    "Attempt to interpret task with default interpreter failed. "
                    "Try to specify interpreter explicitly"
                ) from e
           else:
                raise e

    def prepare(self, *args, **kwargs) -> None:
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

        return self.__task.prepare(*args, **kwargs)

    def run(self, run_id: Optional[str] = None, *args, **kwargs) -> None:
        """Runs the task

        Accepts any arguments and keyword arguments and passes them to the
        underlying callback created in the interpreter itself. For particular
        arguments and keyword arguments, see the documentation of the interpreter
        used before calling this method.
        """
        if not self.__task:
            raise Exception(
                "Unable to run task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return self.__task.run(*args, **kwargs)

    def stop(self, *args, **kwargs) -> None:
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
        return self.__task.stop(*args, **kwargs)

    def results(self, *args, **kwargs) -> list[Result]:
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

        return self.__task.results(*args, **kwargs)

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

    def save(self, path=None) -> str:
        """Bakes the task into a file"""
        if path is None:
            path = os.path.join(
                os.getcwd(),
                f"{self.__tree.reverse_id}.malevichflow"
            )


        # Uploading secrets
        # TODO: upload secrets

        # Dumping flow to json
        flow_data = {
            "tree": self.tree,
            "apps": [
                next(iter(app.keys()))
                for app in ManifestManager().query("dependencies")
            ]
        }
        flow_bytes = pickle.dumps(flow_data)

        with open(path, "wb") as fl:
            fl.write(flow_bytes)

    def get_injectables(self) -> list[BaseInjectable]:
        """Returns a list of possible injections into run

        See the documentation of the corresponding
        interpreter used before calling this method.
        """
        return self.__task.get_injectables()

    def get_operations(self) -> list[str]:
        """Returns a list of operations

        The term operations is defined by the interpreter used.
        See the documentation of the corresponding
        interpreter used before calling this method.
        """
        return self.__task.get_operations()

    def configure(self, operation: str, **kwargs) -> None:
        """Configures the task for a particular operation

        What is configurable and how it is configurable is defined by the
        interpreter used. See the documentation of the corresponding
        interpreter used before calling this method.

        Args:
            operation (str): Operation to configure (one from :meth:`get_operations`)
            **kwargs (Any): Arguments to configure the operation
        """
        return self.__task.configure(operation, **kwargs)

    def get_interpreted_task(self) -> InterpretedTask:
        """Retrieves the interpreted task that is produced when calling :meth:`interpret`

        Returns:
            :class:`InterpretedTask`: The interpreted task
        """  # noqa: E501
        if not self.__task:
            raise Exception(
                "Unable to get interpreted task. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        return self.__task
