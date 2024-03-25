import warnings
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from malevich_space.schema import ComponentSchema

Params = ParamSpec("Params")
R = TypeVar("R")

class FlowFunction(Generic[Params, R]):
    """Class-decorator for a flow function

    Captures the function and holds its metadata.
    """
    allowed_flow_function_kwargs = ComponentSchema.model_fields.keys()

    def __init__(
        self,
        f: Callable[Params, R],
        reverse_id: str,
        name: str,
        description: str,
        **kwargs: Any  # noqa: ANN401
    ) -> None:
        """
        Args:
            f (Callable[Params, R]):
                The function that defines the flow
            reverse_id (str):
                Reverse ID of the flow
            name (str):
                Name of the flow
            description (str): Description of the function
            **kwargs (Any):
                Additional arguments to be passed to the flow component.
                See :class:`ComponentSchema` for details.
        """
        self._Captured = f

        _ignored = []
        for k in kwargs:
            if k not in self.allowed_flow_function_kwargs:
                kwargs.pop(k)
                _ignored.append(k)

        if len(_ignored) > 0:
            warnings.warn(
                f"Ignored unexpected arguments to flow function: {', '.join(_ignored)}"
            )

        self.__component = ComponentSchema(
            reverse_id=reverse_id,
            name=name,
            description=description,
            **kwargs
        )

    @property
    def reverse_id(self) -> str:
        return self.__component.reverse_id

    @property
    def name(self) -> str:
        return self.__component.name

    @property
    def description(self) -> str:
        return self.__component.description

    @property
    def component(self) -> str:
        return self.__component

    def __call__(self, *args: Params.args, **kwds: Params.kwargs) -> R:
        return self._Captured(*args, __component=self.__component, **kwds)



