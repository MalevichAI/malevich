import functools
import inspect
import warnings
from typing import Callable, ParamSpec, TypeVar

from ..models.argument import ArgumentLink
from . import tracer as gn

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
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(
            result, gn.traced) else result

        varnames = func.__code__.co_varnames
        for i, arg in enumerate(args):
            argument_name = varnames[min(i, len(varnames) - 1)]
            if isinstance(arg, gn.traced):
                arg._autoflow.calledby(
                    result,
                    ArgumentLink(index=i, name=argument_name)
                )
        for key in kwargs:
            if isinstance(kwargs[key], gn.traced):
                if key in varnames:
                    kwargs[key]._autoflow.calledby(
                        result, ArgumentLink(index=varnames.index(key), name=key)
                    )
                else:
                    warnings.warn(
                        "Passing a keyword argument to a traced function that is not"
                        " a formal argument of the function is not supported."
                    )

        return result
    return wrapper


def sinktrace(func: Callable[C, R]) -> Callable[C, R]:
    """A special form of autotrace to trace functions with *args

    This decorator is applied to processors that contains
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(result, gn.traced) else result
        parameters = list(inspect.signature(func).parameters.values())
        names = [
            p.name for p in parameters
            if p.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.POSITIONAL_ONLY
            )
        ]

        for i, arg in enumerate(args):
            argument_name = names[min(i, len(names) - 1)]
            if isinstance(arg, gn.traced):
                arg._autoflow.calledby(result, ArgumentLink(index=i, name=argument_name))
            else:
                warnings.warn(
                    "Ignoring non-traced argument in sinktrace function"
                    f" (argument index= {i})"
                )
        return result

    return wrapper
