import functools
from typing import Callable, ParamSpec, TypeVar

import malevich._autoflow.tracer as gn  # engine

C = ParamSpec("C")
R = TypeVar("R")


def autotrace(func: Callable[C, R]) -> Callable[C, R]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
        result = func(*args, **kwargs)
        result = gn.tracer(result) if not isinstance(result, gn.tracer) else result
        assert isinstance(result, gn.tracer), "Function must return a tracer"
        for arg in args:
            argument_name = func.__code__.co_varnames[args.index(arg)]
            if isinstance(arg, gn.tracer):
                arg._autoflow.calledby(result, argument_name)
        return result

    return wrapper
