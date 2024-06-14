import inspect
from itertools import islice
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from ._deploy import Space
from .models.results import SpaceCollectionResult

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class FunctionStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, mapping, reverse_id, deployment_id) -> None:
        self.mapping: dict = mapping
        self.reverse_id = reverse_id
        self.deployment_id = deployment_id
        self._fn_ = fn
        self.__call__ = self._fn_call

    def _fn_call(self, *args, **kwargs) -> list[SpaceCollectionResult]:
        parameters = list(inspect.signature(self._fn_).parameters.values())
        mapped_kwargs = {
            self.mapping.get(k): v
            for k, v in kwargs.items()
        }
        varnames = [
            p.name for p in parameters
            if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        ]
        for arg, key in zip(islice(args, 0, len(varnames)), self.mapping.values()):
            mapped_kwargs[key] = arg

        attach_to_last = kwargs.get('attach_to_last', False)
        deployment_id = kwargs.get('deployment_id', None)

        if not deployment_id:
            if attach_to_last:
                deployment_id = None
            else:
                deployment_id = self.deployment_id

        override = {}

        for key, val in mapped_kwargs.items():
            if val is not None:
                override[key] = val

        print(attach_to_last, deployment_id, override)

        task = Space(
            reverse_id=self.reverse_id,
            deployment_id=deployment_id,
            attach_to_last=attach_to_last,
            force_attach=True
        )
        task.run(override=override, with_logs=True)

        return task.results()

    __call__ = _fn_call

def installed_flow(
    mapping: dict,
    reverse_id:str,
    deployment_id: str
) -> Callable:
    def decorator(fn: Callable) -> FunctionStub[Callable[..., Any], Any]:
        return FunctionStub(fn, mapping, reverse_id, deployment_id)
    return decorator
