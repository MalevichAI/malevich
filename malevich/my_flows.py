import inspect
from itertools import islice
from typing import Any, Callable, Generic, Literal, ParamSpec, TypeVar, overload

from ._deploy import Space
from .models.results import SpaceCollectionResult

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class FlowFunctionStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, reverse_id) -> None:
        self._fn_ = fn
        self.__call__ = self._fn_call
        self.reverse_id = reverse_id

    def _fn_call(self, *args, **kwargs) -> list[SpaceCollectionResult]:
        parameters = list(inspect.signature(self._fn_).parameters.values())
        mapped_kwargs = {
            k: v
            for k, v in kwargs.items()
        }
        varnames = [
            p.name for p in parameters
            if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        ]
        for arg, key in zip(islice(args, 0, len(varnames)), varnames):
            mapped_kwargs[key] = arg
        for p in parameters:
            if (
                p.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD and
                p.name not in mapped_kwargs
            ):
                mapped_kwargs[p.name] = p.default

        task = Space(
            reverse_id=self.reverse_id,
            branch=mapped_kwargs['branch'],
            version=mapped_kwargs['version'],
            deployment_id=mapped_kwargs['run_deployment_id'],
            task_policy=mapped_kwargs['run_task_policy']
        )
        if mapped_kwargs['get_task']:
            return task

        injectables = task.get_injectables()
        overrides = {}
        for col in injectables:
            data = kwargs.get(col.alias, None)
            if data is not None:
                overrides[col.alias] = data
        if not mapped_kwargs['wait_for_results']:
            return task.run(overrides=overrides)

        return task.results()

    __call__ = _fn_call

def installed_flow(reverse_id: str) -> Callable:
    def decorator(fn: Callable) -> FlowFunctionStub[Callable[..., Any], Any]:
        return FlowFunctionStub(fn, reverse_id)
    return decorator
