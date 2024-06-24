import inspect
from itertools import islice
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from malevich_space.schema.flow import InFlowComponentSchema, LoadedOpSchema

from .._autoflow.flow import Flow
from .._deploy import Space
from ..models.results import SpaceCollectionResult

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class FunctionStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, mapping, reverse_id, version) -> None:
        self.mapping: dict = mapping
        self.reverse_id = reverse_id
        self.flow_version = version
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

        attach_to_last = kwargs.get('attach_to_last', True)
        deployment_id = kwargs.get('deployment_id', None)
        get_task = kwargs.get('get_task', False)

        override = {}

        for key, val in mapped_kwargs.items():
            if val is not None:
                override[key] = val

        task = Space(
            reverse_id=self.reverse_id,
            deployment_id=deployment_id,
            attach_to_last=attach_to_last
        )

        if get_task:
            return task

        if task.get_stage().value == 'interpreted':
            task.prepare()

        task.run(override=override, with_logs=True)

        return task.results()

    __call__ = _fn_call

def integration(
    mapping: dict,
    version: str,
    reverse_id:str
) -> Callable:
    def decorator(fn: Callable) -> FunctionStub[Callable[..., Any], Any]:
        return FunctionStub(fn, mapping, reverse_id, version)
    return decorator

