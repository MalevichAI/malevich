import inspect
import json
import warnings
from functools import wraps
from inspect import signature
from typing import Any, Callable, Optional, ParamSpec, TypeVar, overload

import pandas as pd

from malevich._autoflow import Flow, traced
from malevich._utility import Registry, generate_empty_df_from_schema
from malevich.models import (
    ArgumentLink,
    FlowFunction,
    PromisedTask,
    TreeNode,
)
from malevich.types import TracedNode, TracedNodes

from .._ast import boot_flow
from ..models.nodes.morph import MorphNode
from .collection import collection

T = TypeVar("T")
FlowDecoratorReturn = PromisedTask | TracedNode | TracedNodes
Args = ParamSpec("Args")
reg = Registry()

class NoArgumentAnnotation(Warning): ...
class NotTracedArgumentError(Exception): ...
class SubflowArgumentIsDataFrameError(Exception): ...

@overload
def flow(
    fn = None,
    /,
) -> FlowFunction[Args, FlowDecoratorReturn]:
    pass

@overload
def flow(
    *,
    reverse_id: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    dfs_are_collections: Optional[bool] = None,
) -> Callable[[Callable[Args, T]], FlowFunction[Args, FlowDecoratorReturn]]:
    pass

def flow(
    fn = None,
    *,
    reverse_id: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    **kwargs: Any,
) -> Callable[[Callable[Args, T]], FlowFunction[Args, FlowDecoratorReturn]]:
    """Converts a function into a flow

    The function is converted into :class:`FlowFunction <malevich.models.flow_function.FlowFunction>` object that can be
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

    globals_ = inspect.currentframe().f_back.f_globals
    local_vars = inspect.currentframe().f_back.f_locals

    def wrapper(function: Callable[Args, T]) -> FlowFunction[Args, FlowDecoratorReturn]:
        nonlocal reverse_id, name, description
        reverse_id = reverse_id or function.__name__
        function_name = name or function.__name__.replace("_", " ").title()
        description = description or function.__doc__

        if description is None:
            description = f"Meta flow: {function_name}"

        annotations = {}
        sign = signature(function)

        # Check whether the function has variable positional arguments
        # or variable keyword arguments
        if any(
            p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
            for p in sign.parameters.values()
        ):
            raise TypeError(
                "Functions with variable positional or keyword arguments "
                "cannot be converted into flows"
            )

        # Check whether the function has positional-only arguments
        if any(p.kind == p.POSITIONAL_ONLY for p in sign.parameters.values()):
            raise TypeError(
                "Functions with positional-only arguments "
                "cannot be converted into flows"
            )

        # Check whether the function has annotations
        for p in sign.parameters.values():
            if p.annotation is p.empty:
                annotations[p.name] = collection[p.name, None]
                warnings.warn(
                    f"Argument '{p.name}' has no annotation. "
                    f"Assuming it is a collection with name '{p.name}'. "
                    "It is recommended to provide an explicit annotation.",
                    NoArgumentAnnotation,
                )
            else:
                annotations[p.name] = p.annotation

        pos_arg_names = [
            p.name for p in sign.parameters.values() if p.default is p.empty
        ]

        @wraps(function)
        def fn(*args: Args.args, __component, **kwargs: Args.kwargs) -> FlowDecoratorReturn:
            is_subflow = Flow.isinflow()
            args = list(args)
            if is_subflow:
                outer_tracer = traced()

            with Flow():
                named_args = {
                    **{name: value for name, value in zip(pos_arg_names, args)},
                    **kwargs
                }

                traced_args: dict[str, traced] = {}
                traced_shadows: dict[str, traced] = {}
                non_traced_args: dict[str, Any] = {}

                for name, value in named_args.items():
                    if is_subflow and not isinstance(value, traced):
                      raise NotTracedArgumentError(
                          f"You have called a flow function {function.__name__} "
                          "from within another flow function, but the argument "
                          f"{name} is not a valid object. You can call the function "
                          "only with arguments that are results of Malevich operations "
                          "such as other flow functions, collections, or processors."
                      )
                for (name, value), param in zip(
                    named_args.items(), sign.parameters.values()
                ):
                    ann = annotations[name]
                    if hasattr(ann, "__malevich_collection_name__") and (
                        isinstance(value, pd.DataFrame)
                    ):
                        collection_name = ann.__malevich_collection_name__
                        collection_scheme = ann.__malevich_collection_scheme__  # noqa: E501
                    else:
                        raise TypeError(
                            f"Argument '{name}' has invalid annotation. "
                            "It should be a `collection[name]` or nothing."
                        )

                    if isinstance(value, pd.DataFrame):
                        if is_subflow:
                            raise SubflowArgumentIsDataFrameError(
                                "You have called a flow function "
                                f"{function.__name__} from within another flow "
                                f"function, but the argument {name} is a DataFrame. "
                                "You can call the function only with arguments "
                                "that are results of Malevich operations such as "
                                "other flow functions, collections, or processors. "
                                "If you want to pass a DataFrame to the function, "
                                "you should call it from the main context."
                            )
                        else:
                            traced_args[name] = (
                                collection(
                                    df=value,
                                    name=collection_name,
                                    alias=name
                                )
                            )

                            # Substitute with collection for args as well
                            for i in range(len(args)):
                                if args[i] is value:
                                    args[i] = traced_args[name]

                            for k in kwargs:
                                if kwargs[k] is value:
                                    kwargs[k] = traced_args[name]


                    elif isinstance(value, traced):
                        df_ = generate_empty_df_from_schema(
                            collection_scheme
                        )

                        traced_args[name] = collection(
                            collection_name,
                            alias=name,
                            df=df_,
                            scheme=json.dumps(collection_scheme)
                        )

                        traced_shadows[name] = value
                    else:
                        non_traced_args[name] = value

                if is_subflow:
                    traced_pos_args = [
                        traced_args[name] for name in pos_arg_names
                    ]
                    non_traced_pos_args = [
                        non_traced_args[name] for name in pos_arg_names
                    ]
                    traced_kwargs = {
                        name: traced_args[name] for name in kwargs
                        if name not in pos_arg_names
                    }
                    non_traced_kwargs = {
                        name: non_traced_args[name] for name in kwargs
                        if name not in pos_arg_names
                    }

                    __results = boot_flow(
                        function,
                        globals_,
                        local_vars,
                        *[*non_traced_pos_args, *traced_pos_args],
                        **{**non_traced_kwargs, **traced_kwargs}
                    )
                else:
                    __results = boot_flow(
                        function,
                        globals_,
                        local_vars,
                        *args,
                        **kwargs
                    )

                sub_tree = Flow.flow_ref()

                sub_tree.cast_link_types(
                    lambda x: ArgumentLink(
                        **x.model_dump()
                    ) if not isinstance(x, ArgumentLink) else x
                )

                t_node = TreeNode(
                    tree=sub_tree,
                    reverse_id=reverse_id,
                    name=function_name,
                    description=description,
                    results=__results or []
                )

            # The magic starts here
            # ---------------------

            # Case 1: The function is called from the flow
            if is_subflow:
                # Attaching the outer tracer to the node
                outer_tracer.claim(t_node)

                for index, (name, value) in enumerate(named_args.items()):
                    if isinstance(value, traced):
                        # Obtain the bridge nodes: one that connects the argument
                        bridges = sub_tree.edges_from(traced_args[name])
                        bridge_nodes = [(b[2], b[1]) for b in bridges]
                        # Now, bridges are connected
                        _a = ArgumentLink(
                            index=index,
                            name=name,
                            is_compressed_edge=True,
                            compressed_edges=bridge_nodes,
                            shadow_collection=traced_args[name]
                        )
                        traced_shadows[name]._autoflow.calledby(outer_tracer, _a)

                # Check whether len of each item in __results is the same
                for r in __results:
                    if len(r) != len(__results[0]):
                        raise ValueError(
                            "Subflow function returned different number of results "
                            "in return statements. All return statements should return "
                            "the same number of results."
                        )

                morphed_results = []
                len_ = len(__results[0][1]) if isinstance(__results[0][1], tuple) else 1
                for i in range(len_):
                    morph = MorphNode()
                    for morphome in __results:
                        morph.members.append((morphome[0], morphome[1][i].owner))
                    morph.correct_self()
                    morphed_results.append(morph)

                outputs = [
                    traced(
                        TreeNode(
                            **t_node.model_dump(exclude=["underlying_node"]),
                            underlying_node=o,
                        )
                    )
                    for o in morphed_results
                ]

                assert all([o.owner.results is not None for o in outputs])
                return outputs if len(outputs) > 1 else outputs[0]
            else:
                # Save t_node.tree
                return PromisedTask(
                    results=__results, tree=t_node, component=__component
                )

        return FlowFunction(fn, reverse_id, function_name, description, **kwargs)

    return wrapper(fn) if fn else wrapper
