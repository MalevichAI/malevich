from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from pydantic import BaseModel

from .._deploy import Space
from ..models.results.space.collection import SpaceCollectionResult
from ..models.task.interpreted.space import SpaceTask

FnArgs = ParamSpec("FnArgs")
FnReturn = TypeVar("FnReturn")
Config = TypeVar("Config", bound=BaseModel)
ProcConfig = TypeVar("ProcConfig", bound=BaseModel)

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class IntegratedFlowStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(
        self,
        fn,
        reverse_id
    ) -> None:
        self._fn_ = fn
        self.reverse_id = reverse_id

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[True] = True,
        **kwargs
    ) -> SpaceTask:
        pass

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[False] = False,
        **kwargs
    ) -> list[SpaceCollectionResult]:
        pass

    @overload
    def __call__(
        self,
        *args,
        get_task: Literal[False] = False,
        wait_for_results: Literal[False] = False,
        **kwargs
    ) -> str:
        pass

    def __call__(
        self,
        version=None,
        branch=None,
        *args,
        run_deployment_id: str | None = None,
        run_task_policy: Literal['only_use', 'use_or_new', 'no_use'] = 'use_or_new',
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
        expected_keys = {col.alias for col in injectables}
        overrides = {}
        for col in injectables:
            data = kwargs.get(col.alias, None)
            if data is not None:
                overrides[col.alias] = data
                expected_keys.remove(col.alias)

        if expected_keys:
            for key, value in zip(expected_keys, args, strict=False):
                overrides[key] = value

        run_id = task.run(override=overrides, webhook_url=webhook_url)
        if wait_for_results:
            return task.results()
        return run_id



def integrated(reverse_id: str) -> Callable:
    def decorator(fn: Callable) -> IntegratedFlowStub[Callable[..., Any], Any]:
        return IntegratedFlowStub(fn, reverse_id)
    return decorator
