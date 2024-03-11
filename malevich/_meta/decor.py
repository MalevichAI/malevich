from typing import Callable, Generic, ParamSpec, Type, TypeVar

from pydantic import BaseModel

from .._autoflow.function import autotrace, sinktrace
from ..constants import reserved_config_fields

FnArgs = ParamSpec("FnArgs")
FnReturn = TypeVar("FnReturn")
Config = TypeVar("Config", bound=BaseModel)
ProcConfig = TypeVar("ProcConfig", bound=BaseModel)

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class ProcessorFunction(Generic[Config, ProcFunArgs, ProcFunReturn]):
    def __init__(
        self,
        fn: Callable[FnArgs, FnReturn],
        use_sinktrace: bool = False,
        config_model: Type[Config] = BaseModel,
    ) -> None:
        self.base_fn = fn
        if use_sinktrace:
            self.__fn = sinktrace(fn)
        else:
            self.__fn = autotrace(fn)
        self.__config_model = config_model
        self.__call__ = self._fn_call

    def _fn_call(self, *args: FnArgs.args, **kwargs: FnArgs.kwargs) -> FnReturn:
        # FIXME: literal 'config' is overused. Should be replaced with constant.
        if kwargs.get('config', None) is None:
            kwargs['config'] = {}

        if isinstance(kwargs['config'], BaseModel):
            assert type(kwargs['config']) == self.__config_model, (
                f"You have set config={kwargs['config']}, "
                f"but it should be of type {self.__config_model}."
            )

            kwargs['config'] = kwargs['config'].model_dump()
        else:
            assert isinstance(kwargs, dict)

        extra_fields = {**kwargs}
        extra_fields.pop('config')
        for reserved, _ in reserved_config_fields:
            extra_fields.pop(reserved, None)

        kwargs['config'] = {
            **kwargs['config'],
            **extra_fields,
        }

        return self.__fn(*args, **kwargs)

    __call__: Callable[ProcFunArgs, ProcFunReturn] = _fn_call

    @property
    def config(self) -> Type[Config]:
        return self.__config_model


def proc(
    use_sinktrace: bool = False,
    config_model: Type[ProcConfig] = BaseModel,
) -> Callable[
    [Callable[FnArgs, FnReturn]],
    ProcessorFunction[ProcConfig, FnArgs, FnReturn]
]:
    def decorator(
        fn: Callable[FnArgs, FnReturn]
    ) -> ProcessorFunction[ProcConfig, FnArgs, FnReturn]:
        return ProcessorFunction[ProcConfig, FnArgs, FnReturn](
            fn, use_sinktrace, config_model
        )

    return decorator
