import inspect
from itertools import islice
from typing import Callable, Generic, ParamSpec, TypeVar

from ._deploy import Space

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class FunctionStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, mapping, reverse_id) -> None:
        self.mapping: dict = mapping
        self.reverse_id = reverse_id
        self._fn_ = fn
        self.__call__ = self._fn_call

    def _fn_call(self, *args, **kwargs):
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

        deployment_id = kwargs.get('deployment_id', None)
        attach_to_last = kwargs.get('attach_to_last', False)

        override = {}

        for key, val in mapped_kwargs.items():
            if val is not None:
                override[key] = val

        task = Space(
            reverse_id=self.reverse_id,
            deployment_id=deployment_id,
            attach_to_last=attach_to_last
        )
        task.run(override=override)
        return task.results()

    __call__ = _fn_call

def installed_flow(
    mapping: dict,
    reverse_id:str
) -> Callable:
    def decorator(fn: Callable):
        return FunctionStub(fn, mapping, reverse_id)
    return decorator


