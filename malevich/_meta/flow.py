from functools import wraps
from inspect import signature
from typing import Any, Callable, Optional, ParamSpec, TypeVar

import pandas as pd

from .._autoflow.flow import Flow
from .._autoflow.tracer import traced
from .._utility.registry import Registry
from ..models.argument import ArgumentLink
from ..models.flow_function import FlowFunction
from ..models.nodes.base import BaseNode
from ..models.nodes.tree import TreeNode
from ..models.task.promised import PromisedTask
from ..models.types import TracedNode, TracedNodes
from .collection import collection

T = TypeVar("T")
R = PromisedTask | TracedNode | TracedNodes
Args = ParamSpec("Args")
reg = Registry()


def flow(
    fn = None,
    *,
    reverse_id: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    dfs_are_collections: Optional[bool] = None,
    **kwargs: Any,  # noqa: ANN401
) -> Callable[[Callable[Args, T]], FlowFunction[Args, R]] | FlowFunction[Args, R]:
    """Converts a function into a flow

    The function is converted into :class:`malevich.models.flow_function.FlowFunction` object that can be
    called to produce a task or serve as a subflow.

    When the function is called from within another :code:`@flow()` decorated
    function, it returns traced objects to be used as arguments for other
    flow components. When the function is called from the main context, it
    returns a task that can be interpreted by any of available interpreters.

    Args:
        reverse_id (str, optional): Reverse ID of the flow component. Defaults to the name of the function.
        name (str, optional): Name of the flow. Defaults to None.
        description (str, optional): Description of the flow. Defaults to None.
        dfs_are_collections (bool, optional): Whether to treat pandas.DataFrame as a collection. Defaults to False.
        **kwargs (Any):
            Additional arguments to be passed to the flow component.
            See :class:`malevich_space.schema.ComponentSchema` for details.

    Returns:
        Callable[[Callable[Args, T]], Callable[Args, T]]: Decorator for the function.
    """   # noqa: E501

    def wrapper(function: Callable[Args, T]) -> FlowFunction[Args, R]:
        nonlocal reverse_id, name, description

        reverse_id = reverse_id or function.__name__
        name = name or function.__name__.replace("_", " ").title()
        description = description or (function.__doc__.splitlines()[0] if function.__doc__ and len(function.__doc__.splitlines()) > 0 else "")  # noqa: E501

        @wraps(function)
        def fn(*args: Args.args, **kwargs: Args.kwargs) -> R:
            is_subflow = Flow.isinflow()
            args = list(args)
            if is_subflow:
                outer_tracer = traced()

            with Flow() as _tree:
                __hargs = []
                __hkwargs = {}
                for i, i_arg in enumerate(args):
                    if dfs_are_collections and isinstance(i_arg, pd.DataFrame):
                        args[i] = (
                            i_arg := collection(
                                df=i_arg,
                                name=function.__code__.co_varnames[i],
                            )
                        )
                    if isinstance(i_arg, traced):
                        __hargs.append(traced(BaseNode()))
                    else:
                        __hargs.append(i_arg)

                for k in kwargs:
                    k_arg = kwargs[k]
                    if dfs_are_collections and isinstance(k_arg, pd.DataFrame):
                        kwargs[k] = (
                            k_arg := collection(
                                df=k_arg,
                                name=k
                            )
                        )
                    if isinstance(k_arg, traced):
                        __hkwargs[k] = traced(BaseNode())
                    else:
                        __hkwargs[k] = k_arg
                if is_subflow:
                    __results = function(*__hargs, **__hkwargs)
                else:
                    __results = function(*args, **kwargs)
                t_node = TreeNode(
                    tree=_tree,
                    reverse_id=reverse_id,
                    name=name,
                    description=description,
                    results=__results or []
                )

            sign = signature(function)
            params = sign.parameters
            param_values = [*params.values()]
            if is_subflow:
                outer_tracer.claim(t_node)
                for i, i_arg in enumerate(args):
                    if isinstance(i_arg, traced):
                        # In parent tree
                        bridges = _tree.edges_from(__hargs[i])
                        b_nodes = [(b[2], b[1]) for b in bridges]
                        _a = ArgumentLink(
                            index=i,
                            name=param_values[i].name,
                            is_compressed_edge=True,
                            compressed_edges=b_nodes
                        )
                        i_arg._autoflow.calledby(
                            outer_tracer,
                            _a
                        )
                        _tree.prune([__hargs[i]])

                for k in kwargs:
                    k_arg = kwargs[k]
                    if isinstance(k_arg, traced):
                        # New tracer in this tree
                        bridges = _tree.edges_from(__hkwargs[k])
                        b_nodes = [(b[2], b[1]) for b in bridges]
                        i = 0
                        for i, p in enumerate(params.values()):
                            if p.name == k:
                                break
                        _a = ArgumentLink(
                            index=i,
                            name=k,
                            is_compressed_edge=True,
                            compressed_edges=b_nodes
                        )
                        k_arg._autoflow.calledby(outer_tracer, _a)
                        _tree.prune([__hkwargs[k]])
                outputs = [
                    traced(
                        TreeNode(
                            **t_node.model_dump(exclude=["underlying_node"]),
                            underlying_node=o.owner,
                        )
                    )
                    for o in ([__results] if isinstance(
                            __results, traced) else __results
                    )
                    if isinstance(o, traced)
                ]
                assert all([o.owner.results is not None for o in outputs])
                return outputs[0] if len(outputs) == 1 else outputs
            else:
                return PromisedTask(results=__results, tree=t_node)

        return FlowFunction(fn, reverse_id, name, description, **kwargs)

    return wrapper(fn) if fn else wrapper
