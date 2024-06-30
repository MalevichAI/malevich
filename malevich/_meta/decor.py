import inspect
from itertools import islice
from typing import Any, Callable, Generic, ParamSpec, Self, Type, TypeVar

from pydantic import BaseModel

from .._autoflow.function import autotrace, sinktrace
from ..constants import reserved_config_fields
from ..models.type_annotations import ConfigArgument

FnArgs = ParamSpec("FnArgs")
FnReturn = TypeVar("FnReturn")
Config = TypeVar("Config", bound=BaseModel)
ProcConfig = TypeVar("ProcConfig", bound=BaseModel)

ProcFunArgs = ParamSpec("ProcFunArgs")
ProcFunReturn = TypeVar("ProcFunReturn")

class ConfigStruct(dict):
    def __new__(cls, **kwargs) -> Self:
        return dict(**kwargs)

    def __init__(self, **kwargs) -> None:
        """Creates a configuration from keyword arguments"""


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
        self.__required_fields = [
            key
            for key, value in self.__fn.__annotations__.items()
            if hasattr(value, '__metadata__')
            and (metadata := getattr(value, '__metadata__'))
            and isinstance(metadata[0], ConfigArgument)
            and metadata[0].required
        ]

    def _fn_call(self, *args: FnArgs.args, **kwargs: FnArgs.kwargs) -> FnReturn:
        # FIXME: literal 'config' is overused. Should be replaced with constant.
        if kwargs.get('config', None) is None:
            kwargs['config'] = {}

        if isinstance(kwargs['config'], BaseModel):
            assert type(kwargs['config']) == self.__config_model, (  # noqa: E721
                f"You have set config={kwargs['config']}, "
                f"but it should be of type {self.__config_model}."
            )

            kwargs['config'] = kwargs['config'].model_dump()
        else:
            assert isinstance(kwargs, dict)
        reserved_keys = {
            x[0] for x in reserved_config_fields
        }
        extra_fields = {**kwargs}
        extra_fields.pop('config')
        for reserved_keys, _ in reserved_config_fields:
            extra_fields.pop(reserved_keys, None)

        kwargs['config'] = {
            **kwargs['config'],
            **extra_fields,
        }

        kwargs = {
            'config': kwargs['config'],
            **{
                k: v for k, v in kwargs.items()
                if k in reserved_keys
            }
        }

        if (diff_ := set.difference(set(self.__required_fields), kwargs['config'].keys())) != set():  # noqa: E501
            fields_ = ', '.join([f"`{x}`" for x in diff_])
            raise Exception(
                f"Missing required fields {fields_}"
                f" in configuration of the processor `{self.__fn.__name__}`"
            )

        return self.__fn(*args, **kwargs)

    __call__: Callable[ProcFunArgs, ProcFunReturn] = _fn_call

    @property
    def config(self) -> Type[Config] | Type[ConfigStruct]:
        return self.__config_model or ConfigStruct


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


class IntegratedFlowStub(Generic[ProcFunArgs, ProcFunReturn]):
    def __init__(self, fn, mapping, reverse_id, version) -> None:
        self.mapping: dict = mapping
        self.reverse_id = reverse_id
        self.flow_version = version
        self._fn_ = fn
        self.__call__ = self._fn_call

    def _fn_call(self, *args, **kwargs):
        from .._deploy import Space

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
    def decorator(fn: Callable) -> IntegratedFlowStub[Callable[..., Any], Any]:
        return IntegratedFlowStub(fn, mapping, reverse_id, version)
    return decorator

