from functools import wraps
from typing import Callable, Union

from .._autoflow.flow import Flow
from .._autoflow.tracer import traced
from .._utility.registry import Registry
from ..models.nodes import TreeNode
from ..models.task import PromisedTask

reg = Registry()

def flow():  # noqa: ANN201
    def wrapper(function: Callable[[], None]):  # noqa: ANN202
        @wraps(function)
        def fn(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
            with Flow() as _tree:
                __results = function(*args, **kwargs)

            if Flow.isinflow():  # if it is subflow:
                return __results
            else:
                return PromisedTask(results=__results, tree=_tree)
        return fn
    return wrapper
