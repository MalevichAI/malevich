from typing import Any, Callable, Generic, Literal, ParamSpec, TypeVar

from ._deploy import Space
from .models.results import SpaceCollectionResult

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class FlowFunctionStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, reverse_id) -> None:
        self._fn_ = fn
        self.__call__ = self._fn_call
        self.reverse_id = reverse_id

    def _fn_call(
            self,
            version=None,
            branch=None,
            *,
            run_deployment_id: str | None = None,
            run_task_policy: Literal['only_use', 'use_or_new', 'no_use']='use_or_new',
            get_task: bool = False,
            wait_for_results: bool = True,
            webhook_url: str | None = None,
            **kwargs
    ) -> list[SpaceCollectionResult]:
        task = Space(
            reverse_id=self.reverse_id,
            branch=branch,
            version=version,
            deployment_id=run_deployment_id,
            task_policy=run_task_policy
        )
        if get_task:
            return task

        injectables = task.get_injectables()
        overrides = {}
        for col in injectables:
            data = kwargs.get(col.alias, None)
            if data is not None:
                overrides[col.alias] = data
        run_id = task.run(override=overrides, webhook_url=webhook_url)
        if wait_for_results:
            return task.results()
        return run_id


    __call__ = _fn_call

def installed_flow(reverse_id: str) -> Callable:
    def decorator(fn: Callable) -> FlowFunctionStub[Callable[..., Any], Any]:
        return FlowFunctionStub(fn, reverse_id)
    return decorator
