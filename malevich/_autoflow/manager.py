from typing import Iterable

from malevich._autoflow.tracer import ExecutionTree


class FlowManager:
    __flow = None
    def isinflow() -> bool:
        return FlowManager.__flow is not None

    def flow_ref() -> ExecutionTree:
        return FlowManager.__flow


    def __enter__(self) -> None:
        assert FlowManager.__flow is None, "Flow already exists"
        FlowManager.__flow = ExecutionTree()


    def __exit__(self, *args: Iterable) -> None:
        print(FlowManager.__flow.tree)
        FlowManager.__flow = None
