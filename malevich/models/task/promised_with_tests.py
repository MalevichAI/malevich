from collections import defaultdict
from functools import cache
from typing import Callable

from malevich_space.schema import ComponentSchema

from malevich._meta import table
from malevich.models.nodes.operation import OperationNode
from malevich.models.nodes.tree import TreeNode
from malevich.models.task.promised import PromisedTask


class PromisedTaskWithTests(PromisedTask):
    def __init__(self, results, tree: TreeNode, component: ComponentSchema) -> None:
        results = [x for x in tree.tree.nodes() if isinstance(x.owner, OperationNode)]
        super().__init__(results, tree, component)

        self.test_callbacks = defaultdict(list)
        self.result_nodes = [node.owner for node in results]

    @cache
    def _results(self):
        results_ = self.results()
        return {
            node.alias: r.get_dfs()
            for node, r in zip(self.result_nodes, results_)
        }

    def add_test(self, alias: str, test_function: Callable[[list[table]], None]) -> None:
        self.test_callbacks[alias].append((test_function, test_function.__name__))

    def run_all_tests(self) -> bool:
        if not self.test_callbacks:
            return False

        results_ = self._results()
        for key, tests in self.test_callbacks.items():
            for test_function, func_name in tests:
                try:
                    test_function(results_[key])
                except Exception as e_:
                    raise Exception(
                        f'Failed a test {func_name} on operation {key}'
                    ) from e_


        return True
