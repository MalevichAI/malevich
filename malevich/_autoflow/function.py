import functools
from typing import Callable, ParamSpec, TypeVar

from ..models.task import Task  # engine
from . import tracer as gn

C = ParamSpec("C")
R = TypeVar("R")


def autotrace(func: Callable[C, R]) -> Callable[C, Task]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
        result = func(*args, **kwargs)
        result = gn.tracer(result) if not isinstance(result, gn.tracer) else result
        assert isinstance(result, gn.tracer), "Function must return a tracer"
        for arg in args:
            argument_name = func.__code__.co_varnames[args.index(arg)]
            if isinstance(arg, gn.tracer):
                arg._autoflow.calledby(result, (argument_name, args.index(arg)))
        return result

    return wrapper
