import functools
from typing import Callable, ParamSpec, TypeVar

from . import tracer as gn

C = ParamSpec("C")
R = TypeVar("R")


def autotrace(func: Callable[C, R]) -> Callable[C, R]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
        result = func(*args, **kwargs)
        result = gn.traced(result) if not isinstance(result, gn.traced) else result

        for arg in args:
            argument_name = func.__code__.co_varnames[args.index(arg)]
            if isinstance(arg, gn.traced):
                arg._autoflow.calledby(result, (argument_name, args.index(arg)))
        for key in kwargs:
            if isinstance(kwargs[key], gn.traced):
                kwargs[key]._autoflow.calledby(result, (key, kwargs[key]))

        return result

    return wrapper
