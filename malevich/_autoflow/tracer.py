from malevich._autoflow.manager import FlowManager
from malevich._autoflow.tree import ExecutionTree


class autoflow:  # noqa: N801
    def __init__(self, tree: ExecutionTree) -> None:
        self._tree_ref = tree
        self._component_ref = None

    def attach(self, component: str) -> None:
        self._component_ref = component

    def calledby(self, caller: str) -> None:
        self._tree_ref.put_edge(caller, self._component_ref)



class tracked:  # noqa: N801
    __root__ = '__root__'
    def __init__(self, name: str = __root__) -> None:
        assert (
            FlowManager.isinflow()
        ), "Tried to create a tracked variable outside of a flow"

        self.name = name
        self._autoflow = autoflow(
            FlowManager.flow_ref()
        )
        self._autoflow.attach(name)
