import functools
import warnings
from typing import Callable, ParamSpec, TypeVar

from ..models.argument import ArgumentLink
from . import tracer as gn

C = ParamSpec("C")
R = TypeVar("R")


def autotrace(func: Callable[C, R]) -> Callable[C, R]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # noqa: ANN202
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(
            result, gn.traced) else result

        varnames = func.__code__.co_varnames
        for i, arg in enumerate(args):
            argument_name = func.__code__.co_varnames[i]
            if isinstance(arg, gn.traced):
                arg._autoflow.calledby(result, ArgumentLink(index=i, name=argument_name))
        for key in kwargs:
            if isinstance(kwargs[key], gn.traced):
                if key in varnames:
                    kwargs[key]._autoflow.calledby(result, ArgumentLink(index=varnames.index(key), name=key))
                else:
                    warnings.warn(
                        # TODO: Add "See: ..."
                        "Passing a keyword argument to a traced function that is not"
                        " a formal argument of the function is not supported."
                    )

        return result

    return wrapper
