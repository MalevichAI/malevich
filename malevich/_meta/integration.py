from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from pydantic import BaseModel

from malevich._deploy import Space
from malevich._utility import get_space_leaves, unique
from malevich.models import Collection, SpaceCollectionResult, SpaceTask

from .._autoflow.flow import Flow
from .._autoflow.tracer import traced, tracedLike
from ..models.argument import ArgumentLink
from ..models.nodes.collection import CollectionNode
from ..models.nodes.operation import OperationNode
from ..models.nodes.tree import TreeNode

FnArgs = ParamSpec("FnArgs")
FnReturn = TypeVar("FnReturn")
Config = TypeVar("Config", bound=BaseModel)
ProcConfig = TypeVar("ProcConfig", bound=BaseModel)

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class IntegratedFlowStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(
        self,
        fn,
        reverse_id
    ) -> None:
        self._fn_ = fn
        self.reverse_id = reverse_id

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[True] = True,
        **kwargs
    ) -> SpaceTask:
        pass

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[False] = False,
        **kwargs
    ) -> list[SpaceCollectionResult]:
        pass

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[False] = False,
        wait_for_results: Literal[False] = False,
        **kwargs
    ) -> str:
        pass

    def __call__(
        self,
        version=None,
        branch=None,
        *args,
        run_deployment_id: str | None = None,
        run_task_policy: Literal['only_use', 'use_or_new', 'no_use'] = 'use_or_new',
        get_task: bool | None = None,
        wait_for_results: bool | None = None,
        webhook_url: str | None = None,
        **kwargs
    ) -> list[SpaceCollectionResult]:
        task = Space(
            reverse_id=self.reverse_id,
            branch=branch,
            version=version,
            deployment_id=run_deployment_id,
            task_policy=run_task_policy
        )

        injectables = task.get_injectables()
        expected_keys = {col.alias for col in injectables}

        if Flow.isinflow():
            if get_task is not None or wait_for_results is not None:
                raise ValueError(
                    f"You called integrated flow function {self.reverse_id} with "
                    f"get_task or wait_for_results set to a value. This is not allowed "
                    "when calling integrated flow functions from within a "
                    "@flow decorated function. If you intended to run the flow "
                    "or obtain the task, please call the function "
                    " from outside the flow."
                    # TODO: documentation ref
                )

            in_flow_connections: dict[str, traced] = {}
            flow_inputs = {}
            for col in injectables:
                data = kwargs.get(col.alias, None)
                if not isinstance(data, traced):
                    raise ValueError(
                        f"You called integrated flow function {self.reverse_id} "
                        "from within a flow function, but the argument "
                        f"{col.alias} is not a proper value. When defining a flow "
                        "with integrated subflow, you must pass an output of "
                        "Malevich operations such are processors or other flows. "
                        f"The {type(data)} value was passed instead."
                    )

                in_flow_connections[col.alias] = data
                flow_inputs[col.alias] = tracedLike(
                    CollectionNode(
                        alias=col.alias,
                        collection=Collection(collection_id=col.alias)
                    )
                )


            leaves = get_space_leaves(task.component)
            tree_node = TreeNode(
                reverse_id=self.reverse_id,
                alias=unique(self.reverse_id),
                name=task.component.name,
                description=task.component.description,
                integrated=True
            )
            tracer = traced(tree_node)
            for inp in in_flow_connections:
                in_flow_connections[inp]._autoflow.calledby(
                    tracer,
                    ArgumentLink(
                        index=0, # Collection is always the first argument
                        name=inp,
                        is_compressed_edge=True,
                        compressed_edges=[(
                            ArgumentLink(
                                index=0,
                                name=inp,
                            ),
                            flow_inputs[inp]
                        )]
                    )
                )

            traced_leaves = [
                OperationNode(
                    alias=leaf.alias,
                    operation_id=leaf.app.active_op[0].uid,
                )
                for leaf in leaves
            ]

            outputs = [
                traced(TreeNode(
                    **tree_node.model_dump(exclude=['underlying_node']),
                    underlying_node=leaf
                ))
                for leaf in traced_leaves
            ]

            return outputs

        if get_task:
            return task

        if get_task is None:
            get_task = False
        if wait_for_results is None:
            wait_for_results = True

        overrides = {}
        for col in injectables:
            data = kwargs.get(col.alias, None)
            if data is not None:
                overrides[col.alias] = data
                expected_keys.remove(col.alias)

        if expected_keys:
            for key, value in zip(expected_keys, args, strict=False):
                overrides[key] = value

        run_id = task.run(override=overrides, webhook_url=webhook_url)
        if wait_for_results:
            return task.results()
        return run_id



def integrated(reverse_id: str) -> Callable:
    def decorator(fn: Callable) -> IntegratedFlowStub[Callable[..., Any], Any]:
        return IntegratedFlowStub(fn, reverse_id)
    return decorator
