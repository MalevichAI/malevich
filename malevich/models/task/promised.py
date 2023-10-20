
from ..._autoflow.tree import ExecutionTree
from ...interpreter.abstract import Interpreter
from ...models.task.base import BaseTask
from ...models.types import FlowOutput


class PromisedTask(BaseTask):
    def __init__(self, results: FlowOutput, tree: ExecutionTree) -> None:
        self.__results = results
        self.__tree = tree
        self.__task = None

    def interpret(self, interpreter: Interpreter) -> None:
        task = interpreter.interpret(self.__tree)
        task.commit_returned(self.__results)
        self.__task = task

    def prepare(self) -> None:
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.prepare()


    def run(self) -> None:
        if not self.__task:
            raise Exception(
                "Unable to run task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return self.__task.run()


    def stop(self) -> None:
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return self.__task.stop()


    def results(self) -> None:
        if not self.__task:
            raise Exception(
                "Unable to get results of the task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.results()


    def commit_returned(self, returned: FlowOutput) -> None:
        if not self.__task:
            raise Exception(
                "Unable to commit returned results to the task, "
                "that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.commit_returned(returned)
