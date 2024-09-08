import functools
import inspect
import warnings
from itertools import islice
from typing import Callable, ParamSpec, TypeVar


from . import tracer as gn
from .link import AutoflowLink

C = ParamSpec("C")
R = TypeVar("R")


def autotrace(func: Callable[C, R]) -> Callable[C, R]:
    """Function decorator that enables automatic dependency tracking

    The result is turned into :func:`traced <malevich._autoflow.tracer.traced>`
    object (if it is not already).

    Then, all arguments that are traced are linked to the result
    by calling :func:`traced.calledby`
    method with results being the 'caller'.

    The link produced is :class:`ArgumentLink <malevich.models.argument.ArgumentLink`
    with ``index`` set to the argument position and ``name`` set to the argument name.
    If the argument is passed as a keyword argument,
    and the function accepts the argument as
    **kwargs, raises a warning and does not link the argument.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Call function to obtain resuts
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(
            result, gn.traced # If the results is not traced, trace it
        ) else result
        # Obtain parameters from function signature.
        parameters = list(inspect.signature(func).parameters.values())
        # !! The function only traces positional only arguments
        # which are the ones that preceed \
        # def func(arg1, arg2, /, arg3, *, ...) <-- only arg1 and arg2 are
        # considered
        varnames = [ # filter pos-only args
            p.name for p in parameters
            if p.kind == inspect.Parameter.POSITIONAL_ONLY

        ]
        # tracing arguments, slicing up to known args
        new_edges = 0
        for i, arg in enumerate(islice(args, 0, len(varnames))):
            argument_name = varnames[i]
            if isinstance(arg, gn.traced):
                arg._autoflow.calledby(
                    result,
                    AutoflowLink(index=i, name=argument_name)
                )
                new_edges += 1
            else:
                raise ValueError(
                    f"You passed invalid argument to {func.__name__} "
                    f"at position {i}. When using processors, you may only "
                    "pass specific objects produced by Malevich operations."
                    # TODO: documentation ref
                )

        if not new_edges:
            result._autoflow._tree_ref().add_node(result)

        return result
    return wrapper


def sinktrace(func: Callable[C, R]) -> Callable[C, R]:
    """A special form of autotrace to trace functions with *args

    This decorator is applied to processors that contains
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> gn.traced:
        # Call function to obtain resuts
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(result, gn.traced) else result
        parameters = list(inspect.signature(func).parameters.values())
        names = [
            p.name for p in parameters
            if p.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.POSITIONAL_ONLY
            )
        ]
        new_edges = 0
        for i, arg in enumerate(args):
            real_index = min(i, len(names) - 1)
            argument_name = names[real_index]
            if isinstance(arg, gn.traced):
                new_edges += 1
                arg._autoflow.calledby(result, AutoflowLink(
                        index=real_index,
                        name=argument_name,
                        in_sink=i >= len(names) - 1
                    )
                )
            else:
                warnings.warn(
                    "Ignoring non-traced argument in sinktrace function"
                    f" (argument index= {i})"
                )
        if not new_edges:
            result._autoflow._tree_ref().add_node(result)

        return result

    return wrapper
