
import os
import pickle

from ...interpreter.abstract import Interpreter
from ...interpreter.space import SpaceInterpreter
from ...manifest import ManifestManager
from ...models.nodes.tree import TreeNode
from ...models.task.base import BaseTask
from ...models.types import FlowOutput
from ..injections import BaseInjectable


class PromisedTask(BaseTask):
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
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.prepare(*args, **kwargs)

    def run(self, *args, **kwargs) -> None:
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
        if not self.__task:
            raise Exception(
                "Unable to prepare task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )
        # TODO: try/except with error on this level
        # if task is not prepared
        return self.__task.stop(*args, **kwargs)

    def results(self, *args, **kwargs) -> None:
        if not self.__task:
            raise Exception(
                "Unable to get results of the task, that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.results(*args, **kwargs)

    def commit_returned(self, returned: FlowOutput) -> None:
        if not self.__task:
            raise Exception(
                "Unable to commit returned results to the task, "
                "that has not been interpreted. "
                "Please, use `.interpret` first to attach task to "
                "a particular platform"
            )

        return self.__task.commit_returned(returned)

    def save(self, path=None) -> str:
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
            "apps": [next(iter(app.keys())) for app in ManifestManager().query("dependencies")]
        }
        flow_bytes = pickle.dumps(flow_data)

        with open(path, "wb") as fl:
            fl.write(flow_bytes)

    def get_injectables(self) -> list[BaseInjectable]:
        return self.__task.get_injectables()
