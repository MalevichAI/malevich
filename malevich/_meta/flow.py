from functools import wraps
from typing import Any, Callable

from .._autoflow.flow import Flow
from .._autoflow.tracer import traced
from .._utility.registry import Registry
from ..interpreter.abstract import Interpreter
from ..models.nodes.base import BaseNode
from ..models.nodes.tree import TreeNode
from ..models.task.promised import PromisedTask

reg = Registry()

def flow(reverse_id=None, name=None):  # noqa: ANN201
    def wrapper(function: Callable[[], Any]):  # noqa: ANN202
        nonlocal reverse_id, name
        reverse_id = reverse_id or function.__name__
        name = name or function.__name__.replace('_', ' ').title()

        @wraps(function)
        def fn(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
            is_subflow = Flow.isinflow()
            if is_subflow:
                outer_tracer = traced()
                outer_tree = Flow.flow_ref()
                args = list(args)

            with Flow() as _tree:
                __hargs = []
                __hkwargs = {}
                for i, i_arg in enumerate(args):
                    if isinstance(i_arg, traced):
                        __hargs.append(
                            traced(BaseNode())
                        )
                    else:
                        __hargs.append(i_arg)

                for k in kwargs:
                    k_arg = kwargs[k]
                    if isinstance(k_arg, traced):
                        __hkwargs[k] = traced(BaseNode())
                    else:
                        __hkwargs[k] = k_arg

                __results = function(*__hargs, **__hkwargs)
                t_node = TreeNode(
                    tree=_tree,
                    reverse_id=reverse_id,
                    name=name,
                )

            if is_subflow:
                outer_tracer.claim(t_node)
                for i, i_arg in enumerate(args):
                    if isinstance(i_arg, traced):
                        # In parent tree
                        bridges = _tree.edges_from(__hargs[i])
                        b_nodes = [(b[1], b[2]) for b in bridges]
                        i_arg._autoflow.calledby(outer_tracer, (i, b_nodes))
                        _tree.prune([__hargs[i]])

                for k in kwargs:
                    k_arg = kwargs[k]
                    if isinstance(k_arg, traced):
                        # New tracer in this tree
                        bridges = _tree.edges_from(__hkwargs[k])
                        b_nodes = [(b[1], b[2]) for b in bridges]
                        k_arg._autoflow.calledby(outer_tracer, (i, b_nodes))
                        _tree.prune([__hkwargs[k]])


                outputs = [
                    traced(
                        TreeNode(
                            **t_node.model_dump(exclude=["underlying_node"]),
                            underlying_node=o.owner,
                        )
                    )
                    for o in ([__results] if isinstance(__results, traced) else __results)
                    if isinstance(o, traced)
                ]

                # for o in outputs:
                #     if isinstance(o, traced):
                #         o.claim(t_node)


            # if is_subflow:
                return outputs[0] if len(outputs) == 1 else outputs
            else:
                return PromisedTask(results=__results, tree=_tree)
        return fn
    return wrapper
